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

  DEAL_DUPLICATE_FIELD: "FormID",
  VERSION: "r254C3d",
  LOCAL_ACTIVATION_ENABLED: false,
  LOCAL_COMMIT_ENABLED: false,
  DEPLOY_VERSION_NUMBER: 254,
  PROJECT: "FODE_Main_Runtime"
};

function canonicalExecBase_() {
  const raw = String(ScriptApp.getService().getUrl() || "").trim();
  const m = raw.match(/\/macros\/s\/([a-zA-Z0-9\-_]+)\/exec/);
  return m
    ? "https://script.google.com/macros/s/" + m[1] + "/exec"
    : raw;
}

function doGet(e) {
  const view = String((e && e.parameter && e.parameter.view) || "").trim().toLowerCase();
  const base = canonicalExecBase_();

  const body = view === "whoami"
    ? {
        ok: true,
        project: CONFIG.PROJECT,
        VERSION: CONFIG.VERSION,
        DEPLOY_VERSION_NUMBER: CONFIG.DEPLOY_VERSION_NUMBER,
        script_url: base,
        intake_url: base,
        timestamp: new Date().toISOString()
      }
    : {
        ok: true,
        project: CONFIG.PROJECT,
        message: "GET alive; use POST for intake",
        VERSION: CONFIG.VERSION,
        DEPLOY_VERSION_NUMBER: CONFIG.DEPLOY_VERSION_NUMBER,
        url: base,
        timestamp: new Date().toISOString()
      };

  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}
/******************** ENTRYPOINT ********************/
function doPost(e) {

  const ss = SpreadsheetApp.openById(CONFIG.SHEET_ID);
  const logSheet = mustGetSheet_(ss, CONFIG.LOG_SHEET);

  const payload = getPayload_(e);
  const validation = validatePayloadShape_(payload);

  if (!validation.ok) {
    log_(logSheet, "PAYLOAD_INVALID", validation.reason);
    return ContentService.createTextOutput(JSON.stringify({
      status: "error",
      reason: validation.reason
    })).setMimeType(ContentService.MimeType.JSON);
  }

  const sheet = mustGetSheet_(ss, CONFIG.DATA_SHEET);
  const existingRow = findExistingByFormId_(sheet, validation.key);

  if (existingRow) {
    log_(logSheet, "IDEMPOTENT_HIT", JSON.stringify({
      formId: validation.key,
      row: existingRow
    }));
  }

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
  let prepResult = null;

  try {
    prepResult = prepareLocalActivationLocked_(ss, forwarded, {
      correlationId,
      folder,
      folderUrl,
      crmResponse,
      contactId,
      dealId,
      adapterTimestamp
    });

    log_(logSheet, "LOCAL_ACTIVATION_PREP", JSON.stringify(prepResult));
  } catch (err) {
    log_(logSheet, "LOCAL_ACTIVATION_PREP_ERROR", JSON.stringify({
      correlation_id: correlationId,
      error: err.message
    }));
  }


  try {
    log_(logSheet, "FORWARD_PAYLOAD_TRACE", JSON.stringify({
      correlation_id: correlationId,
      fd_form_id: fdFormId,
      keys: Object.keys(forwarded || {}),
      hasFolderUrl: !!forwarded.Folder_Url,
      hasCRM: !!forwarded.CRM_Response,
      hasContact: !!forwarded.Contact_ID,
      hasDeal: !!forwarded.Deal_ID
    }));

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
    log_(logSheet, "FORWARD_RESPONSE_TRACE", JSON.stringify({
      correlation_id: correlationId,
      responseCode: responseCode,
      hasParsed: !!parsed,
      status: parsed ? parsed.status : "",
      applicantId: parsed ? parsed.ApplicantID : "",
      rawLength: (responseText || "").length
    }));


    const ok = responseCode === 200 && parsed && parsed.status === "ok";

    if (!ok) {
      log_(logSheet, "FORWARD_ERROR_TRACE", JSON.stringify({
        correlation_id: correlationId,
        responseCode: responseCode,
        responseText: (responseText || "").slice(0, 1000)
      }));
      log_(logSheet, "FORWARD_FAIL", responseText);
      return ContentService.createTextOutput(JSON.stringify({ status: "error" }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    log_(logSheet, "FORWARD_SUCCESS_TRACE", JSON.stringify({
      correlation_id: correlationId,
      fd_form_id: fdFormId,
      applicantId: parsed ? parsed.ApplicantID : ""
    }));
    if (prepResult && parsed && parsed.ApplicantID) {
      log_(logSheet, "PARITY_COMPARE_STRONG", JSON.stringify({
        correlation_id: correlationId,
        duplicateDetectedUnderLock: !!prepResult.duplicate,
        existingRow: prepResult.existingRow || 0,
        applicantId_local: prepResult.applicantId_local || "",
        applicantId_downstream: parsed.ApplicantID,
        incomingFolderUrl: prepResult.incomingFolderUrl || "",
        folderMode_local: prepResult.folderMode_local || "",
        portalSecretsPrepared: !!(prepResult.tokenPlan && prepResult.tokenPlan.portalSecretsPrepared),
        tokenDerivationBasis: prepResult.tokenPlan ? (prepResult.tokenPlan.tokenDerivationBasis || "") : "",
        requiredHeadersPresent: !!prepResult.requiredHeadersPresent,
        verificationMode: prepResult.verificationPlan ? (prepResult.verificationPlan.verificationMode || "") : "",
        canFallbackAfterWrite: prepResult.verificationPlan ? !!prepResult.verificationPlan.canFallbackAfterWrite : false,
        mappedHeaderCount: prepResult.mappedHeaderCount || 0,
      }));
    }
    try {
      const dataSheet = mustGetSheet_(ss, CONFIG.DATA_SHEET);
      const committedRow = readRowByApplicantId_(dataSheet, parsed.ApplicantID);

      if (committedRow) {
        const committedSnapshot = extractParitySnapshot_(committedRow.rowMap);
        log_(logSheet, "DOWNSTREAM_ROW_SNAPSHOT", JSON.stringify({
          correlation_id: correlationId,
          applicantId: parsed.ApplicantID,
          row: committedRow.row,
          snapshot: committedSnapshot
        }));

        if (prepResult) {
          const localTokenTrace = buildLocalTokenDerivationTrace_(prepResult);
          const committedToken = extractCommittedTokenSnapshot_(committedSnapshot);

          log_(logSheet, "TOKEN_DERIVATION_TRACE_LOCAL", JSON.stringify({
            correlation_id: correlationId,
            applicantId: parsed.ApplicantID,
            local: localTokenTrace
          }));

          log_(logSheet, "TOKEN_DERIVATION_TRACE_COMMITTED", JSON.stringify({
            correlation_id: correlationId,
            applicantId: parsed.ApplicantID,
            committed: committedToken
          }));

          const tokenParity = diagnoseTokenParity_(localTokenTrace, committedToken);
          log_(logSheet, "TOKEN_PARITY_DIAG", JSON.stringify({
            correlation_id: correlationId,
            applicantId: parsed.ApplicantID,
            tokenParity: tokenParity
          }));

          const parity = comparePrepToCommittedRow_(prepResult, committedSnapshot);
          log_(logSheet, "PARITY_COMPARE_FINAL", JSON.stringify({
            correlation_id: correlationId,
            applicantId: parsed.ApplicantID,
            parity: parity
          }));
        }
      } else {
        log_(logSheet, "DOWNSTREAM_ROW_SNAPSHOT_MISS", JSON.stringify({
          correlation_id: correlationId,
          applicantId: parsed.ApplicantID
        }));
      }
    } catch (err) {
      log_(logSheet, "DOWNSTREAM_ROW_SNAPSHOT_ERROR", JSON.stringify({
        correlation_id: correlationId,
        applicantId: parsed ? parsed.ApplicantID : "",
        error: err.message
      }));
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
    log_(logSheet, "FORWARD_EXCEPTION_TRACE", JSON.stringify({
      correlation_id: correlationId,
      error: err.message
    }));
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

function validatePayloadShape_(p) {
  if (!p) return { ok: false, reason: "empty payload" };

  const fd = String(p.FD_FormID || p.FormID || "").trim();
  if (!fd) return { ok: false, reason: "missing FormID" };

  if (!p.First_Name || !p.Last_Name) {
    return { ok: false, reason: "missing name fields" };
  }

  return { ok: true, key: fd };
}

function findExistingByFormId_(sheet, formId) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const idx = headers.indexOf("FD_FormID") !== -1
    ? headers.indexOf("FD_FormID")
    : headers.indexOf("FormID");

  if (idx === -1) return null;

  const values = sheet.getRange(2, idx + 1, lastRow - 1, 1).getValues();

  for (let i = values.length - 1; i >= 0; i--) {
    if (String(values[i][0]).trim() === formId) {
      return i + 2;
    }
  }
  return null;
}

function findExistingActivationByFormIdLocked_(sheet, formId) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const fdIdx = headers.indexOf("FD_FormID");
  const formIdx = headers.indexOf("FormID");
  if (fdIdx === -1 && formIdx === -1) return null;

  const applicantIdx = headers.indexOf("ApplicantID");
  const folderIdx = headers.indexOf("Folder_Url");
  const values = sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).getValues();

  for (let i = values.length - 1; i >= 0; i--) {
    const fdVal = fdIdx !== -1 ? String(values[i][fdIdx] || "").trim() : "";
    const formVal = formIdx !== -1 ? String(values[i][formIdx] || "").trim() : "";
    if (fdVal === formId || formVal === formId) {
      return {
        row: i + 2,
        existingApplicantId: applicantIdx !== -1 ? String(values[i][applicantIdx] || "").trim() : "",
        existingFolderUrl: folderIdx !== -1 ? String(values[i][folderIdx] || "").trim() : ""
      };
    }
  }

  return null;
}

function toHex_(bytes) {
  return bytes.map(function(b) {
    const v = (b + 256) % 256;
    return ("0" + v.toString(16)).slice(-2);
  }).join("");
}

function buildLocalActivationState_() {
  return {
    duplicate: false,
    writeStarted: false,
    targetRow: 0,
    postWriteVerified: false,
    preWriteFailure: false,
    postWriteFailure: false
  };
}

function preparePortalSecretParity_(sheet, payload, ctx, applicantId) {
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const hasPortalTokenHashHeader = headers.indexOf("PortalTokenHash") !== -1;
  const hasPortalTokenIssuedAtHeader = headers.indexOf("PortalTokenIssuedAt") !== -1;
  const formId = String(payload.FD_FormID || payload.FormID || "").trim();
  const portalTokenIssuedAt = String(ctx.adapterTimestamp || new Date().toISOString());
  const tokenDerivationBasis = "applicantId|formId|correlationId|adapterTimestamp";

  if (!hasPortalTokenHashHeader && !hasPortalTokenIssuedAtHeader) {
    return {
      hasPortalTokenHashHeader,
      hasPortalTokenIssuedAtHeader,
      portalTokenHash: "",
      portalTokenIssuedAt: "",
      portalSecretsPrepared: false,
      tokenSource: "local_parity_prep_current_assumption",
      tokenDerivationBasis: tokenDerivationBasis,
      reason: "portal token headers absent"
    };
  }

  if (!applicantId || !formId) {
    return {
      hasPortalTokenHashHeader,
      hasPortalTokenIssuedAtHeader,
      portalTokenHash: "",
      portalTokenIssuedAt: portalTokenIssuedAt,
      portalSecretsPrepared: false,
      tokenSource: "local_parity_prep_current_assumption",
      tokenDerivationBasis: tokenDerivationBasis,
      reason: "missing applicantId or formId for token parity prep"
    };
  }

  const digestInput = [applicantId, formId, String(ctx.correlationId || ""), portalTokenIssuedAt].join("|");
  const portalTokenHash = toHex_(Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, digestInput));

  return {
    hasPortalTokenHashHeader,
    hasPortalTokenIssuedAtHeader,
    portalTokenHash: hasPortalTokenHashHeader ? portalTokenHash : "",
    portalTokenIssuedAt: hasPortalTokenIssuedAtHeader ? portalTokenIssuedAt : "",
    portalSecretsPrepared: hasPortalTokenHashHeader && hasPortalTokenIssuedAtHeader,
    tokenSource: "local_parity_prep_current_assumption",
    tokenDerivationBasis: tokenDerivationBasis,
    reason: hasPortalTokenHashHeader && hasPortalTokenIssuedAtHeader ? "" : "partial token headers present"
  };
}

function buildExactActivationRowPlan_(sheet, payload, ctx) {
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const intakeValue = String(payload["Intake Year"] || payload.Intake_Year || payload.IntakeYear || "").trim();
  const desired = {
    ApplicantID: String(ctx.applicantId || "").trim(),
    FD_FormID: String(payload.FD_FormID || payload.FormID || "").trim(),
    FormID: String(payload.FormID || payload.FD_FormID || "").trim(),
    First_Name: String(payload.First_Name || "").trim(),
    Last_Name: String(payload.Last_Name || "").trim(),
    Grade_Applying_For: String(payload.Grade_Applying_For || "").trim(),
    Parent_Email: String(payload.Parent_Email || "").trim(),
    Parent_Phone: String(payload.Parent_Phone || "").trim(),
    Province_Of_Birth: String(payload.Province_Of_Birth || "").trim(),
    "Intake Year": intakeValue,
    Intake_Year: intakeValue,
    Folder_Url: String(ctx.folderUrl || (ctx.folder ? ctx.folder.getUrl() : "") || "").trim(),
    CRM_Response: String(ctx.crmResponse || "").trim(),
    Contact_ID: String(ctx.contactId || "").trim(),
    Deal_ID: String(ctx.dealId || "").trim(),
    adapter_forwarded: String(payload.adapter_forwarded || "1").trim(),
    adapter_source: String(payload.adapter_source || "sheet_bound_adapter").trim(),
    correlation_id: String(ctx.correlationId || payload.correlation_id || "").trim(),
    adapter_timestamp: String(ctx.adapterTimestamp || payload.adapter_timestamp || "").trim(),
    PortalTokenHash: String((ctx.tokenPlan && ctx.tokenPlan.portalTokenHash) || "").trim(),
    PortalTokenIssuedAt: String((ctx.tokenPlan && ctx.tokenPlan.portalTokenIssuedAt) || "").trim()
  };

  const requiredHeadersChecked = [
    "ApplicantID",
    "FD_FormID",
    "FormID",
    "Folder_Url",
    "CRM_Response",
    "Contact_ID",
    "Deal_ID",
    "adapter_forwarded",
    "adapter_source",
    "correlation_id",
    "adapter_timestamp"
  ];

  if (headers.indexOf("Intake Year") !== -1) {
    requiredHeadersChecked.push("Intake Year");
  } else if (headers.indexOf("Intake_Year") !== -1) {
    requiredHeadersChecked.push("Intake_Year");
  } else {
    requiredHeadersChecked.push("Intake Year");
  }

  if (ctx.tokenPlan && ctx.tokenPlan.hasPortalTokenHashHeader) requiredHeadersChecked.push("PortalTokenHash");
  if (ctx.tokenPlan && ctx.tokenPlan.hasPortalTokenIssuedAtHeader) requiredHeadersChecked.push("PortalTokenIssuedAt");

  const rowFieldsPlanned = {};
  let mappedHeaderCount = 0;

  headers.forEach(function(header) {
    let value = "";
    if (Object.prototype.hasOwnProperty.call(desired, header)) {
      value = desired[header];
      if (requiredHeadersChecked.indexOf(header) !== -1) mappedHeaderCount++;
    } else if (Object.prototype.hasOwnProperty.call(payload, header)) {
      value = normalize_(payload[header]);
    }
    rowFieldsPlanned[header] = value == null ? "" : String(value);
  });

  const missingHeaders = requiredHeadersChecked.filter(function(header) { return headers.indexOf(header) === -1; });

  return {
    rowFieldsPlanned: rowFieldsPlanned,
    missingHeaders: missingHeaders,
    mappedHeaderCount: mappedHeaderCount,
    requiredHeadersPresent: missingHeaders.length === 0,
    requiredHeadersChecked: requiredHeadersChecked
  };
}

function buildActivationVerificationPlan_(sheet, rowPlan, tokenPlan) {
  const rowFields = rowPlan.rowFieldsPlanned || {};
  const requiredNonBlankHeaders = [];

  ["ApplicantID", "Folder_Url"].forEach(function(header) {
    if (Object.prototype.hasOwnProperty.call(rowFields, header)) requiredNonBlankHeaders.push(header);
  });

  if (Object.prototype.hasOwnProperty.call(rowFields, "FD_FormID")) {
    requiredNonBlankHeaders.push("FD_FormID");
  } else if (Object.prototype.hasOwnProperty.call(rowFields, "FormID")) {
    requiredNonBlankHeaders.push("FormID");
  }

  if (tokenPlan && tokenPlan.hasPortalTokenHashHeader) requiredNonBlankHeaders.push("PortalTokenHash");
  if (tokenPlan && tokenPlan.hasPortalTokenIssuedAtHeader) requiredNonBlankHeaders.push("PortalTokenIssuedAt");

  return {
    requiredNonBlankHeaders: requiredNonBlankHeaders,
    targetApplicantId: String(rowFields.ApplicantID || "").trim(),
    targetFormId: String(rowFields.FD_FormID || rowFields.FormID || "").trim(),
    tokenChecksRequired: {
      portalTokenHashRequired: !!(tokenPlan && tokenPlan.hasPortalTokenHashHeader),
      portalTokenIssuedAtRequired: !!(tokenPlan && tokenPlan.hasPortalTokenIssuedAtHeader)
    },
    folderRequired: true,
    verificationMode: "post_write_required",
    canFallbackAfterWrite: false
  };
}

function prepareLocalActivationPlanNoLock_(sheet, payload, ctx) {
  const formId = String(payload.FD_FormID || payload.FormID || "").trim();
  const duplicateHit = formId ? findExistingActivationByFormIdLocked_(sheet, formId) : null;
  const activationStatePlan = buildLocalActivationState_();

  if (duplicateHit) {
    const duplicateApplicantId = String(duplicateHit.existingApplicantId || "").trim();
    const tokenPlanDup = preparePortalSecretParity_(sheet, payload, ctx, duplicateApplicantId);
    const rowPlanDup = buildExactActivationRowPlan_(sheet, payload, {
      correlationId: ctx.correlationId,
      folder: ctx.folder,
      folderUrl: duplicateHit.existingFolderUrl || ctx.folderUrl,
      crmResponse: ctx.crmResponse,
      contactId: ctx.contactId,
      dealId: ctx.dealId,
      adapterTimestamp: ctx.adapterTimestamp,
      applicantId: duplicateApplicantId,
      tokenPlan: tokenPlanDup
    });
    const verificationPlanDup = buildActivationVerificationPlan_(sheet, rowPlanDup, tokenPlanDup);
    activationStatePlan.duplicate = true;

    return {
      correlation_id: ctx.correlationId,
      duplicate: true,
      existingRow: duplicateHit.row,
      existingApplicantId: duplicateApplicantId,
      existingFolderUrl: duplicateHit.existingFolderUrl || "",
      applicantId_local: duplicateApplicantId,
      applicantIdStats: null,
      incomingFolderUrl: String(ctx.folderUrl || "").trim(),
      folderMode_local: (duplicateHit.existingFolderUrl || ctx.folderUrl) ? "trust_incoming_for_now" : "would_create_new",
      tokenPlan: tokenPlanDup,
      rowFieldsPlanned: rowPlanDup.rowFieldsPlanned,
      missingHeaders: rowPlanDup.missingHeaders,
      mappedHeaderCount: rowPlanDup.mappedHeaderCount,
      requiredHeadersPresent: rowPlanDup.requiredHeadersPresent,
      requiredHeadersChecked: rowPlanDup.requiredHeadersChecked,
      verificationPlan: verificationPlanDup,
      activationStatePlan: activationStatePlan
    };
  }

  const sim = simulateNextApplicantId_(sheet);
  const tokenPlan = preparePortalSecretParity_(sheet, payload, ctx, sim.applicantId);
  const rowPlan = buildExactActivationRowPlan_(sheet, payload, {
    correlationId: ctx.correlationId,
    folder: ctx.folder,
    folderUrl: ctx.folderUrl,
    crmResponse: ctx.crmResponse,
    contactId: ctx.contactId,
    dealId: ctx.dealId,
    adapterTimestamp: ctx.adapterTimestamp,
    applicantId: sim.applicantId,
    tokenPlan: tokenPlan
  });
  const verificationPlan = buildActivationVerificationPlan_(sheet, rowPlan, tokenPlan);

  return {
    correlation_id: ctx.correlationId,
    duplicate: false,
    existingRow: 0,
    existingApplicantId: "",
    existingFolderUrl: "",
    applicantId_local: sim.applicantId,
    applicantIdStats: {
      validCount: sim.validCount,
      maxSuffix: sim.maxSuffix,
      skippedBlankCount: sim.skippedBlankCount,
      skippedMalformedCount: sim.skippedMalformedCount
    },
    incomingFolderUrl: String(ctx.folderUrl || "").trim(),
    folderMode_local: ctx.folderUrl ? "trust_incoming_for_now" : "would_create_new",
    tokenPlan: tokenPlan,
    rowFieldsPlanned: rowPlan.rowFieldsPlanned,
    missingHeaders: rowPlan.missingHeaders,
    mappedHeaderCount: rowPlan.mappedHeaderCount,
    requiredHeadersPresent: rowPlan.requiredHeadersPresent,
    requiredHeadersChecked: rowPlan.requiredHeadersChecked,
    verificationPlan: verificationPlan,
    activationStatePlan: activationStatePlan
  };
}

function prepareLocalActivationLocked_(ss, payload, ctx) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const sheet = mustGetSheet_(ss, CONFIG.DATA_SHEET);
    return prepareLocalActivationPlanNoLock_(sheet, payload, ctx);
  } finally {
    lock.releaseLock();
  }
}

function readRowByApplicantId_(sheet, applicantId) {
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const idx = headers.indexOf("ApplicantID");
  if (idx === -1) return null;

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  const values = sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).getValues();

  for (var i = values.length - 1; i >= 0; i--) {
    const rowApplicantId = String(values[i][idx] || "").trim();
    if (rowApplicantId === String(applicantId || "").trim()) {
      const rowMap = {};
      for (var j = 0; j < headers.length; j++) rowMap[headers[j]] = values[i][j];
      return {
        row: i + 2,
        rowMap: rowMap
      };
    }
  }

  return null;
}

function extractParitySnapshot_(rowMap) {
  rowMap = rowMap || {};
  return {
    ApplicantID: String(rowMap["ApplicantID"] || "").trim(),
    FD_FormID: String(rowMap["FD_FormID"] || "").trim(),
    FormID: String(rowMap["FormID"] || "").trim(),
    Folder_Url: String(rowMap["Folder_Url"] || "").trim(),
    CRM_Response: String(rowMap["CRM_Response"] || "").trim(),
    Contact_ID: String(rowMap["Contact_ID"] || "").trim(),
    Deal_ID: String(rowMap["Deal_ID"] || "").trim(),
    PortalTokenHash: String(rowMap["PortalTokenHash"] || "").trim(),
    PortalTokenIssuedAt: String(rowMap["PortalTokenIssuedAt"] || "").trim(),
    adapter_forwarded: String(rowMap["adapter_forwarded"] || "").trim(),
    adapter_source: String(rowMap["adapter_source"] || "").trim(),
    correlation_id: String(rowMap["correlation_id"] || "").trim(),
    adapter_timestamp: String(rowMap["adapter_timestamp"] || "").trim(),
    intake_year_value: String(
      rowMap["Intake Year"] || rowMap["Intake_Year"] || rowMap["IntakeYear"] || ""
    ).trim()
  };
}

function buildLocalTokenDerivationTrace_(prepResult) {
  const tokenPlan = (prepResult && prepResult.tokenPlan) || {};
  const rowPlan = (prepResult && prepResult.rowFieldsPlanned) || {};

  return {
    applicantId_local: String(prepResult && prepResult.applicantId_local || "").trim(),
    fdFormId: String(rowPlan["FD_FormID"] || rowPlan["FormID"] || "").trim(),
    correlation_id: String(rowPlan["correlation_id"] || "").trim(),
    adapter_timestamp: String(rowPlan["adapter_timestamp"] || "").trim(),
    portalTokenIssuedAt_local: String(tokenPlan.portalTokenIssuedAt || "").trim(),
    portalTokenHash_local: String(tokenPlan.portalTokenHash || "").trim(),
    tokenSource: String(tokenPlan.tokenSource || "").trim(),
    tokenDerivationBasis: String(tokenPlan.tokenDerivationBasis || "").trim()
  };
}

function extractCommittedTokenSnapshot_(committedSnapshot) {
  committedSnapshot = committedSnapshot || {};
  return {
    ApplicantID: String(committedSnapshot.ApplicantID || "").trim(),
    FD_FormID: String(committedSnapshot.FD_FormID || committedSnapshot.FormID || "").trim(),
    correlation_id: String(committedSnapshot.correlation_id || "").trim(),
    adapter_timestamp: String(committedSnapshot.adapter_timestamp || "").trim(),
    portalTokenIssuedAt_committed: String(committedSnapshot.PortalTokenIssuedAt || "").trim(),
    portalTokenHash_committed: String(committedSnapshot.PortalTokenHash || "").trim()
  };
}

function diagnoseTokenParity_(localTrace, committedToken) {
  localTrace = localTrace || {};
  committedToken = committedToken || {};

  return {
    applicantIdMatch: String(localTrace.applicantId_local || "").trim() === String(committedToken.ApplicantID || "").trim(),
    fdFormIdMatch: String(localTrace.fdFormId || "").trim() === String(committedToken.FD_FormID || "").trim(),
    correlationIdMatch: String(localTrace.correlation_id || "").trim() === String(committedToken.correlation_id || "").trim(),
    adapterTimestampMatch: String(localTrace.adapter_timestamp || "").trim() === String(committedToken.adapter_timestamp || "").trim(),
    portalTokenIssuedAtMatch: String(localTrace.portalTokenIssuedAt_local || "").trim() === String(committedToken.portalTokenIssuedAt_committed || "").trim(),
    portalTokenHashMatch: String(localTrace.portalTokenHash_local || "").trim() === String(committedToken.portalTokenHash_committed || "").trim(),
    localIssuedAtLooksIso: /T\d{2}:\d{2}:\d{2}/.test(String(localTrace.portalTokenIssuedAt_local || "")),
    committedIssuedAtLooksIso: /T\d{2}:\d{2}:\d{2}/.test(String(committedToken.portalTokenIssuedAt_committed || "")),
    localIssuedAtLength: String(localTrace.portalTokenIssuedAt_local || "").length,
    committedIssuedAtLength: String(committedToken.portalTokenIssuedAt_committed || "").length,
    issuedAtFormatMismatch: String(localTrace.portalTokenIssuedAt_local || "").trim() !== String(committedToken.portalTokenIssuedAt_committed || "").trim(),
    hashMismatch: String(localTrace.portalTokenHash_local || "").trim() !== String(committedToken.portalTokenHash_committed || "").trim()
  };
}
function comparePrepToCommittedRow_(prepResult, committedSnapshot) {
  const rowPlan = (prepResult && prepResult.rowFieldsPlanned) || {};
  const tokenPlan = (prepResult && prepResult.tokenPlan) || {};
  const committed = committedSnapshot || {};

  return {
    applicantIdMatch: String(prepResult && prepResult.applicantId_local || "").trim() === String(committed.ApplicantID || "").trim(),
    folderUrlMatch: String(rowPlan["Folder_Url"] || "").trim() === String(committed.Folder_Url || "").trim(),
    crmResponseMatch: String(rowPlan["CRM_Response"] || "").trim() === String(committed.CRM_Response || "").trim(),
    contactIdMatch: String(rowPlan["Contact_ID"] || "").trim() === String(committed.Contact_ID || "").trim(),
    dealIdMatch: String(rowPlan["Deal_ID"] || "").trim() === String(committed.Deal_ID || "").trim(),
    adapterForwardedMatch: String(rowPlan["adapter_forwarded"] || "").trim() === String(committed.adapter_forwarded || "").trim(),
    adapterSourceMatch: String(rowPlan["adapter_source"] || "").trim() === String(committed.adapter_source || "").trim(),
    correlationIdMatch: String(rowPlan["correlation_id"] || "").trim() === String(committed.correlation_id || "").trim(),
    adapterTimestampMatch: String(rowPlan["adapter_timestamp"] || "").trim() === String(committed.adapter_timestamp || "").trim(),
    portalTokenHashMatch: String(tokenPlan.portalTokenHash || "").trim() === String(committed.PortalTokenHash || "").trim(),
    portalTokenIssuedAtMatch: String(tokenPlan.portalTokenIssuedAt || "").trim() === String(committed.PortalTokenIssuedAt || "").trim(),
    intakeYearMatch: String(
      rowPlan["Intake Year"] || rowPlan["Intake_Year"] || ""
    ).trim() === String(committed.intake_year_value || "").trim()
  };
}
function simulateNextApplicantId_(sheet) {
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const idx = headers.indexOf("ApplicantID");
  if (idx === -1) throw new Error("ApplicantID column missing");

  const lastRow = sheet.getLastRow();
  const yy = String(new Date().getFullYear()).slice(-2);
  if (lastRow < 2) {
    return {
      applicantId: "FODE-" + yy + "-000001",
      validCount: 0,
      maxSuffix: 0,
      skippedBlankCount: 0,
      skippedMalformedCount: 0
    };
  }

  const values = sheet.getRange(2, idx + 1, lastRow - 1, 1).getValues();
  let validCount = 0, maxSuffix = 0, skippedBlankCount = 0, skippedMalformedCount = 0;

  for (let i = 0; i < values.length; i++) {
    const v = String(values[i][0] || "").trim();
    if (!v) { skippedBlankCount++; continue; }
    const m = v.match(/^([A-Z]+-\d{2}-)(\d+)$/);
    if (!m) { skippedMalformedCount++; continue; }
    validCount++;
    maxSuffix = Math.max(maxSuffix, parseInt(m[2], 10));
  }

  return {
    applicantId: "FODE-" + yy + "-" + String(maxSuffix + 1).padStart(6, "0"),
    validCount,
    maxSuffix,
    skippedBlankCount,
    skippedMalformedCount
  };
}

function nextApplicantIdLocked_(sheet) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
    const idx = headers.indexOf("ApplicantID");
    if (idx === -1) throw new Error("ApplicantID column missing");

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return "FODE-" + new Date().getFullYear().toString().slice(-2) + "-000001";

    const values = sheet.getRange(2, idx + 1, lastRow - 1, 1).getValues();

    for (let i = values.length - 1; i >= 0; i--) {
      const v = String(values[i][0] || "").trim();
      const m = v.match(/^([A-Z]+-\d{2}-)(\d+)$/);
      if (m) {
        const next = String(parseInt(m[2], 10) + 1).padStart(m[2].length, "0");
        return m[1] + next;
      }
    }

    return "FODE-" + new Date().getFullYear().toString().slice(-2) + "-000001";

  } finally {
    lock.releaseLock();
  }
}

function verifyApplicantWritten_(sheet, applicantId) {
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const idx = headers.indexOf("ApplicantID");
  if (idx === -1) return false;

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return false;

  const values = sheet.getRange(2, idx + 1, lastRow - 1, 1).getValues();

  for (let i = values.length - 1; i >= 0; i--) {
    if (String(values[i][0]).trim() === applicantId) return true;
  }
  return false;
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
function buildShadowRowPlan_(payload, ctx) {
  return {
    FD_FormID: String(payload.FD_FormID || payload.FormID || "").trim(),
    FormID: String(payload.FormID || payload.FD_FormID || "").trim(),
    First_Name: String(payload.First_Name || "").trim(),
    Last_Name: String(payload.Last_Name || "").trim(),
    Grade_Applying_For: String(payload.Grade_Applying_For || "").trim(),
    Parent_Email: String(payload.Parent_Email || "").trim(),
    Parent_Phone: String(payload.Parent_Phone || "").trim(),
    Province_Of_Birth: String(payload.Province_Of_Birth || "").trim(),
    Intake_Year: String(payload["Intake Year"] || payload.Intake_Year || payload.IntakeYear || "").trim(),
    Folder_Url: String(ctx.folderUrl || "").trim(),
    CRM_Response_Present: !!ctx.crmResponse,
    Contact_ID: String(ctx.contactId || "").trim(),
    Deal_ID: String(ctx.dealId || "").trim(),
    adapter_forwarded: "1",
    adapter_source: "sheet_bound_adapter",
    correlation_id: String(ctx.correlationId || "").trim(),
    adapter_timestamp: String(ctx.adapterTimestamp || "").trim()
  };
}

function buildShadowPrereqPlan_(sheet, incomingFolderUrl) {
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const hasPortalTokenHashHeader = headers.indexOf("PortalTokenHash") !== -1;
  const hasPortalTokenIssuedAtHeader = headers.indexOf("PortalTokenIssuedAt") !== -1;

  return {
    incomingFolderUrl: incomingFolderUrl || "",
    folderMode_local: incomingFolderUrl ? "trust_incoming_for_now" : "would_create_new",
    tokenFieldsPlanned: {
      hasPortalTokenHashHeader,
      hasPortalTokenIssuedAtHeader,
      wouldNeedPortalSecretPreparation: hasPortalTokenHashHeader && hasPortalTokenIssuedAtHeader
    },
    verificationChecksPlanned: [
      "ApplicantID persisted",
      "Folder_Url present",
      "PortalTokenHash present if header exists",
      "PortalTokenIssuedAt present if header exists"
    ]
  };
}

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








