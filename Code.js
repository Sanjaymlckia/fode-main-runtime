/************************************************************
FODE ADMISSIONS — FINAL STABLE PRODUCTION SCRIPT (v10.0)
************************************************************/

/******************** CONFIG ********************/
const CONFIG = {
  SHEET_ID: "1fHmeGNmpOj9PEPQ5Fp4tUyCP4UdH70lltukraD4SalU",
  DATA_SHEET: "FODE_Data",
  LOG_SHEET: "Webhook_Log",

  ROOT_FOLDER_ID: "1vGD3DoOv1hlxYoTIfrNCZqAnrVKmghuB",
  YEAR_FOLDER: "2025",

  ZOHO_API_BASE: "https://www.zohoapis.com",
  ZOHO_OAUTH_BASE: "https://accounts.zoho.com",

  BRAND: "FODE",
  DEAL_STAGE: "New To MLCKIA",

  DEAL_DUPLICATE_FIELD: "FormID"
};

function doGet(e) {
  return ContentService.createTextOutput(JSON.stringify({
    ok: true,
    project: "FODE_Main_Runtime",
    message: "GET alive; use POST for intake",
    timestamp: new Date().toISOString()
  })).setMimeType(ContentService.MimeType.JSON);
}
/******************** ENTRYPOINT ********************/
function doPost(e) {

  const ss = SpreadsheetApp.openById(CONFIG.SHEET_ID);
  const logSheet = mustGetSheet_(ss, CONFIG.LOG_SHEET);

  const payload = getPayload_(e);
  const fdFormId = String(payload.FD_FormID || payload.FormID || "").trim();
  const correlationId =
    String(payload.correlation_id || payload.FD_FormID || payload.FormID || "").trim() ||
    Utilities.getUuid();
  const adapterTimestamp = new Date().toISOString();

  log_(logSheet, "POST HIT", payloadSummary_(payload));
  log_(logSheet, "FORWARD_START", JSON.stringify({
    correlation_id: correlationId,
    fd_form_id: fdFormId
  }));

  let contactId = "";
  let dealId = "";
  let crmResponse = "";
  let folderUrl = "";
  let folder = null;

  try {
    folder = createApplicantFolder_(payload);
    folderUrl = folder ? folder.getUrl() : "";

    const token = getZohoToken_();

    const contactRes = upsertZohoContact_(token, payload);
    contactId = contactRes.id || "";

    const dealRes = upsertZohoDeal_(token, payload, folder, contactId);
    dealId = dealRes.id || "";

    crmResponse = JSON.stringify({
      contact: contactRes.raw,
      deal: dealRes.raw
    });

    log_(logSheet, "ZOHO OK", `contactId=${contactId} dealId=${dealId}`);

  } catch (err) {
    crmResponse = "ERROR: " + err.message;
    log_(logSheet, "ADAPTER_PREPROCESS_ERROR", crmResponse);
  }

  const forwarded = Object.assign({}, payload);

  delete forwarded.view;
  delete forwarded.route;
  delete forwarded.action;
  delete forwarded._action;

  forwarded.adapter_forwarded = "1";
  forwarded.adapter_source = "sheet_bound_adapter";
  forwarded.correlation_id = correlationId;
  forwarded.adapter_timestamp = adapterTimestamp;

  if (folderUrl) forwarded.Folder_Url = folderUrl;
  if (crmResponse) forwarded.CRM_Response = crmResponse;
  if (contactId) forwarded.Contact_ID = contactId;
  if (dealId) forwarded.Deal_ID = dealId;

  const mainUrl = PropertiesService.getScriptProperties().getProperty("MAIN_PORTAL_INTAKE_URL");

  if (!mainUrl) {
    log_(logSheet, "FORWARD_FAIL", "Missing MAIN_PORTAL_INTAKE_URL");
    return ContentService.createTextOutput(JSON.stringify({ status: "error" }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  let responseCode = 0;
  let responseText = "";

  try {
    const resp = UrlFetchApp.fetch(mainUrl, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(forwarded),
      muteHttpExceptions: true
    });

    responseCode = resp.getResponseCode();
    responseText = resp.getContentText() || "";

    let parsed = null;
    try { parsed = JSON.parse(responseText); } catch {}

    const ok = responseCode === 200 && parsed && parsed.status === "ok";

    if (!ok) {
      log_(logSheet, "FORWARD_FAIL", responseText);
      return ContentService.createTextOutput(JSON.stringify({ status: "error" }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    log_(logSheet, "FORWARD_OK", JSON.stringify({
      correlation_id: correlationId,
      fd_form_id: fdFormId
    }));

    return ContentService.createTextOutput(JSON.stringify({
      status: "ok",
      ApplicantID: parsed.ApplicantID || ""
    })).setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    log_(logSheet, "FORWARD_FAIL", err.message);
    return ContentService.createTextOutput(JSON.stringify({ status: "error" }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

/******************** PAYLOAD ********************/
function getPayload_(e) {
  if (e?.parameter && Object.keys(e.parameter).length) return e.parameter;
  if (e?.postData?.contents) {
    try { return JSON.parse(e.postData.contents); } catch {}
  }
  return {};
}

/******************** DRIVE ********************/
function createApplicantFolder_(payload) {
  const first = slug_(payload.First_Name);
  const last  = slug_(payload.Last_Name);
  const date  = new Date().toISOString().slice(0,10);

  const root = DriveApp.getFolderById(CONFIG.ROOT_FOLDER_ID);
  const year = getOrCreateFolder_(root, CONFIG.YEAR_FOLDER);

  return year.createFolder(`${first}_${last}_${date}`);
}

function getOrCreateFolder_(parent, name) {
  const it = parent.getFoldersByName(name);
  return it.hasNext() ? it.next() : parent.createFolder(name);
}

/******************** ZOHO ********************/
function getZohoToken_() {
  const p = PropertiesService.getScriptProperties();
  const res = UrlFetchApp.fetch(CONFIG.ZOHO_OAUTH_BASE + "/oauth/v2/token", {
    method: "post",
    payload: {
      refresh_token: p.getProperty("ZOHO_REFRESH_TOKEN"),
      client_id: p.getProperty("ZOHO_CLIENT_ID"),
      client_secret: p.getProperty("ZOHO_CLIENT_SECRET"),
      grant_type: "refresh_token"
    }
  });
  const j = JSON.parse(res.getContentText());
  if (!j.access_token) throw new Error("Zoho OAuth failed");
  return j.access_token;
}

function upsertZohoContact_(token, payload) {
  const contact = {
    First_Name: clean_(payload.First_Name),
    Last_Name: clean_(payload.Last_Name),
    Email: clean_(payload.Parent_Email),
    Phone: clean_(payload.Parent_Phone),
    Contact_Brand: CONFIG.BRAND,
    Province_of_Birth: clean_(payload.Province_Of_Birth)
  };

  const dup = [];
  if (contact.Email) dup.push("Email");
  if (contact.Phone) dup.push("Phone");

  const res = UrlFetchApp.fetch(CONFIG.ZOHO_API_BASE + "/crm/v2/Contacts/upsert", {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Zoho-oauthtoken " + token },
    payload: JSON.stringify({
      data: [contact],
      duplicate_check_fields: dup
    })
  });

  const j = JSON.parse(res.getContentText());
  return {
    id: j?.data?.[0]?.details?.id || "",
    raw: j
  };
}

function upsertZohoDeal_(token, payload, folder, contactId) {
  const grade = clean_(payload.Grade_Applying_For);

  let intakeYear = clean_(
    payload["Intake Year"] || payload.Intake_Year || payload.IntakeYear
  );
  if (!intakeYear) intakeYear = String(new Date().getFullYear() + 1);

  const formId = clean_(payload.FormID || payload.FD_FormID) || "FODE_" + Date.now();

  const deal = {
    Deal_Name: `FODE ${grade} – ${clean_(payload.First_Name)} ${clean_(payload.Last_Name)}`,
    Stage: CONFIG.DEAL_STAGE,
    Brand: CONFIG.BRAND,
    Type: CONFIG.BRAND,

    Grade_Applying_For: grade,
    "Intake Year": intakeYear,

    Parent_Phone: clean_(payload.Parent_Phone),
    Parent_Email: clean_(payload.Parent_Email),
    Province_of_Birth: clean_(payload.Province_Of_Birth),
    Subjects_Selected: normalize_(payload.Subjects_Selected),

    Folder_URL: folder.getUrl(),
    FormID: formId
  };

  if (contactId) deal.Contact_Name = { id: contactId };

  deal.Owner = {
    id: PropertiesService.getScriptProperties().getProperty("FODE_DEFAULT_OWNER_ID")
  };

  const res = UrlFetchApp.fetch(CONFIG.ZOHO_API_BASE + "/crm/v2/Deals/upsert", {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Zoho-oauthtoken " + token },
    payload: JSON.stringify({
      data: [deal],
      duplicate_check_fields: [CONFIG.DEAL_DUPLICATE_FIELD]
    })
  });

  const j = JSON.parse(res.getContentText());
  return {
    id: j?.data?.[0]?.details?.id || "",
    raw: j
  };
}

/******************** UTIL ********************/
function mustGetSheet_(ss, name) {
  const sh = ss.getSheetByName(name);
  if (!sh) throw new Error("Missing sheet: " + name);
  return sh;
}

function clean_(v) {
  return v == null ? "" : String(v).trim();
}

function normalize_(v) {
  if (v == null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function slug_(s) {
  return clean_(s).toLowerCase().replace(/[^a-z0-9]+/g, "_");
}

function log_(sheet, label, msg) {
  sheet.appendRow([new Date(), label, msg || ""]);
}

function payloadSummary_(p) {
  return JSON.stringify({
    First_Name: p.First_Name,
    Last_Name: p.Last_Name,
    Grade: p.Grade_Applying_For,
    Intake: p["Intake Year"] || ""
  });
}
