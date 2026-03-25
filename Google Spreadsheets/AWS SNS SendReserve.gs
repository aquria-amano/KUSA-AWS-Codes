// --- 共通設定（スクリプトプロパティから取得） ---
const AWS_ACCESS_KEY = PropertiesService.getScriptProperties().getProperty('AWS_ACCESS_KEY');
const AWS_SECRET_KEY = PropertiesService.getScriptProperties().getProperty('AWS_SECRET_KEY');
const BROADCAST_API_KEY = PropertiesService.getScriptProperties().getProperty('BROADCAST_API_KEY');

// リージョン列の定義 (D列=4, E列=5...)
const REGIONS = [
  { name: "AsiaEast",      col: 4 }, 
  { name: "AsiaChina",     col: 5 }, 
  { name: "AsiaSoutheast", col: 6 }, 
  { name: "EuropeCentral", col: 7 }, 
  { name: "GMT",           col: 8 }, 
  { name: "USEast",        col: 9 }, 
  { name: "USWest",        col: 10 }
];

/**
 * 1. 即時送信 (D列〜J列のチェックボックスを対象)
 */
function sendPushNotifications() {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const data = sheet.getRange("A2:J16").getValues();
  
  const targets = [];
  REGIONS.forEach(reg => {
    for (let i = 0; i < data.length; i++) {
      if (data[i][reg.col - 1] === true) { 
        targets.push({
          topic: `kusa-${reg.name}-${data[i][0].toString().trim()}`,
          title: data[i][1].toString(),
          body: data[i][2].toString(),
          regionName: reg.name,
          lang: data[i][0].toString()
        });
      }
    }
  });

  if (targets.length === 0) {
    ui.alert("送信対象（リージョン列のチェックボックス）が選択されていません。");
    return;
  }

  // 即時通知の確認ダイアログ
  const confirm = ui.alert('即時送信確認', targets.length + ' 件の通知を即時送信します。よろしいですか？', ui.ButtonSet.YES_NO);
  if (confirm !== ui.Button.YES) return;

  const apiUrl = "https://vo5uvo595c.execute-api.ap-northeast-1.amazonaws.com/broadcast";
  let successCount = 0;

  targets.forEach((t, index) => {
    const payload = { 
      "targetTopic": t.topic,
      "title": t.title,
      "body": t.body
    };

    if (index > 0) {
      Utilities.sleep(200); // 通常の待機
    }
    
    const options = {
      "method": "post", "contentType": "application/json",
      "headers": { "x-api-key": BROADCAST_API_KEY },
      "payload": JSON.stringify(payload), "muteHttpExceptions": true 
    };

    try {
      // --- 修正箇所: 通常のfetchからリトライ付きのfetchへ変更 ---
      const res = fetchWithRetry(apiUrl, options);
      if(res.getResponseCode() === 200) successCount++;
      saveLogToSheet(payload, `即時送信(${t.regionName}): ` + res.getResponseCode());
    } catch (e) {
      // 最大回数リトライしても失敗した場合のみここに来る
      saveLogToSheet(payload, "即時送信 最終エラー: " + e.toString());
    }
  });

  clearCheckboxes();
  ui.alert(successCount + ' 件の送信リクエストを完了しました。');
}

/**
 * 追加：リトライ用補助関数 (指数バックオフ)
 */
function fetchWithRetry(url, options, maxRetries = 3) {
  let lastError;
  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = UrlFetchApp.fetch(url, options);
      const code = response.getResponseCode();
      
      // 成功、もしくはリトライしても無駄なエラー(403等)はそのまま返す
      if (code === 200 || code === 403) return response;
      
      // それ以外(帯域制限エラー等)はリトライ対象
      console.warn(`リトライ試行 ${i + 1}: Code ${code}`);
    } catch (e) {
      lastError = e;
      console.warn(`リトライ試行 ${i + 1}: ${e.toString()}`);
    }
    
    if (i < maxRetries - 1) {
      // 失敗するごとに待ち時間を増やす (1秒 → 2秒)
      Utilities.sleep(Math.pow(2, i) * 1000);
    }
  }
  throw lastError || new Error("リトライ上限に達しました");
}

/**
 * 2. 予約送信 (変更なし)
 */
function createNotificationSchedule() {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  
  const data = sheet.getRange("A2:J16").getValues();    
  const master = sheet.getRange("L2:P8").getValues();   

  let scheduleQueue = []; 
  const now = new Date();
  const tenMinutesLater = new Date(now.getTime() + 10 * 60 * 1000);

  try {
    // A. 予約対象の抽出とバリデーション
    REGIONS.forEach((reg, index) => {
      const regionTargets = [];
      let jstTimeValue = master[index][4]; // P列 (index 4)

      for (let i = 0; i < data.length; i++) {
        if (data[i][reg.col - 1] === true) {
          regionTargets.push({
            lang: data[i][0].toString().trim(),
            title: data[i][1].toString(),
            body: data[i][2].toString()
          });
        }
      }

      if (regionTargets.length > 0) {
        if (!(jstTimeValue instanceof Date) && jstTimeValue !== "") {
          jstTimeValue = new Date(jstTimeValue);
        }

        if (!jstTimeValue || isNaN(jstTimeValue.getTime())) {
          throw new Error(reg.name + " の予約日時(P列)が正しくありません。");
        }
        
        if (jstTimeValue < tenMinutesLater) {
          throw new Error(reg.name + " の予約時間は現在より10分以上先に設定してください。");
        }

        scheduleQueue.push({
          regionName: reg.name,
          jstTime: jstTimeValue,
          notifications: regionTargets
        });
      }
    });

    if (scheduleQueue.length === 0) {
      ui.alert("予約対象のチェックボックスが入っていません。");
      return;
    }

    // B. 予約確認ダイアログ
    const confirmMsg = scheduleQueue.map(s => 
      `・${s.regionName}: ${Utilities.formatDate(s.jstTime, "JST", "MM/dd HH:mm")} (${s.notifications.length}言語)`
    ).join("\n");

    const response = ui.alert(
      '予約登録の確認', 
      `以下の内容で AWS に予約を登録します。よろしいですか？\n\n${confirmMsg}`, 
      ui.ButtonSet.YES_NO
    );

    if (response !== ui.Button.YES) return;

    // C. AWS Scheduler への登録
    let successCount = 0;
    scheduleQueue.forEach(s => {
      const dateStr = Utilities.formatDate(s.jstTime, "JST", "yyyy-MM-dd'T'HH:mm:ss");
      const scheduleName = "kusa-reg-" + s.regionName + "-" + Utilities.formatDate(new Date(), "JST", "MMdd-HHmmss");
      
      const payload = {
        "timezoneLabel": s.regionName,
        "notifications": s.notifications
      };

      const awsPayload = {
        "Name": scheduleName,
        "ClientToken": scheduleName, 
        "ScheduleExpression": "at(" + dateStr + ")",
        "ScheduleExpressionTimezone": "Asia/Tokyo",
        "Target": {
          "Arn": "arn:aws:lambda:ap-northeast-1:518045402321:function:kusa-PushNotification-Broadcaster",
          "RoleArn": "arn:aws:iam::518045402321:role/kusa-PushNotification-Broadcast-Role",
          "Input": JSON.stringify(payload)
        },
        "ActionAfterCompletion": "DELETE",
        "FlexibleTimeWindow": { "Mode": "OFF" }
      };

      callAwsScheduler(awsPayload, "ap-northeast-1");
      saveLogToSheet(payload, "予約成功: " + s.regionName + " (JST: " + dateStr + ")");
      successCount++;
    });

    clearCheckboxes();
    ui.alert(successCount + ' 件のリージョン予約を完了しました。');

  } catch (e) {
    ui.alert("エラー: " + e.message);
  }
}

/**
 * 追加：D列〜J列のすべてのチェックボックスをONにする
 */
function checkAllBoxes() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  // 2行目、4列目(D列)から、15行分(16行目まで)、7列分(J列まで)をすべてtrueに
  sheet.getRange(2, 4, 15, 7).setValue(true);
}

/**
 * 補助：チェックボックスをすべてOFFにする (D列〜J列)
 */
function clearCheckboxes() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  sheet.getRange(2, 4, 15, 7).setValue(false);
}

/**
 * 共通：AWS署名計算 (Scheduler用)
 */
function callAwsScheduler(payload, region) {
  const service = 'scheduler';
  const host = service + '.' + region + '.amazonaws.com';
  const canonicalUri = '/schedules/' + payload.Name;
  const method = 'POST';
  const datetime = Utilities.formatDate(new Date(), "GMT", "yyyyMMdd'T'HHmmss'Z'");
  const date = datetime.substr(0, 8);
  const payloadBytes = Utilities.newBlob(JSON.stringify(payload)).getBytes();
  const hashedPayload = bytesToHex_(Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, payloadBytes));
  const canonicalRequest = [method, canonicalUri, '', 'host:' + host, 'x-amz-date:' + datetime, '', 'host;x-amz-date', hashedPayload].join('\n');
  const credentialScope = [date, region, service, 'aws4_request'].join('/');
  const stringToSign = ['AWS4-HMAC-SHA256', datetime, credentialScope, bytesToHex_(Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, canonicalRequest))].join('\n');
  
  const dateBytes = Utilities.newBlob(date).getBytes();
  const keyBytes = Utilities.newBlob("AWS4" + AWS_SECRET_KEY).getBytes();
  const kDate = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, dateBytes, keyBytes);
  const kRegion = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, Utilities.newBlob(region).getBytes(), kDate);
  const kService = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, Utilities.newBlob(service).getBytes(), kRegion);
  const kSigning = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, Utilities.newBlob("aws4_request").getBytes(), kService);
  const signature = bytesToHex_(Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, Utilities.newBlob(stringToSign).getBytes(), kSigning));
  
  const authHeader = 'AWS4-HMAC-SHA256 Credential=' + AWS_ACCESS_KEY + '/' + credentialScope + ', SignedHeaders=host;x-amz-date, Signature=' + signature;
  const options = { 
    method: method, 
    contentType: 'application/json', 
    headers: { 'Authorization': authHeader, 'X-Amz-Date': datetime }, 
    payload: payloadBytes, 
    muteHttpExceptions: true 
  };
  
  const response = UrlFetchApp.fetch('https://' + host + canonicalUri, options);
  if (response.getResponseCode() !== 200) throw new Error("AWS Error: " + response.getContentText());
}

/**
 * 共通：履歴保存
 */
function saveLogToSheet(payload, result) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const logSheet = ss.getSheetByName("送信履歴") || ss.insertSheet("送信履歴");
  const userEmail = Session.getActiveUser().getEmail();
  
  logSheet.appendRow([
    Utilities.formatDate(new Date(), "JST", "yyyy/MM/dd HH:mm:ss"), 
    userEmail, 
    JSON.stringify(payload), 
    result
  ]);
}

/**
 * 補助：16進数変換
 */
function bytesToHex_(bytes) {
  return bytes.map(function(byte) { return ('0' + (byte & 0xFF).toString(16)).slice(-2); }).join('');
}