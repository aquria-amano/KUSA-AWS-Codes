// --- 共通設定（スクリプトプロパティから取得） ---
const AWS_ACCESS_KEY = PropertiesService.getScriptProperties().getProperty('AWS_ACCESS_KEY');
const AWS_SECRET_KEY = PropertiesService.getScriptProperties().getProperty('AWS_SECRET_KEY');
const BROADCAST_API_KEY = PropertiesService.getScriptProperties().getProperty('BROADCAST_API_KEY');

/**
 * 1. 即時送信 (チェックボックスONのものを対象)
 */
function sendPushNotifications() {
  const ui = SpreadsheetApp.getUi();
  const { schedules } = getSchedulesFromSheet();
  
  const allNotifications = [];
  for (let timeKey in schedules) {
    allNotifications.push(...schedules[timeKey]);
  }

  if (allNotifications.length === 0) {
    ui.alert("送信対象（チェックボックス）が選択されていません。");
    return;
  }

  const response = ui.alert('即時送信確認', allNotifications.length + ' 件の通知を即時送信します。よろしいですか？', ui.ButtonSet.YES_NO);
  if (response !== ui.Button.YES) return;

  const payload = { "notifications": allNotifications };
  const apiUrl = "https://vo5uvo595c.execute-api.ap-northeast-1.amazonaws.com/broadcast";
  const options = {
    "method": "post", "contentType": "application/json",
    "headers": { "x-api-key": BROADCAST_API_KEY },
    "payload": JSON.stringify(payload), "muteHttpExceptions": true 
  };

  try {
    const res = UrlFetchApp.fetch(apiUrl, options);
    saveLogToSheet(payload, "即時送信成功: " + res.getResponseCode());
    
    // ★ 成功したのでチェックボックスをすべて外す
    clearCheckboxes();
    
    ui.alert('送信完了');
  } catch (e) { 
    saveLogToSheet(payload, "即時送信エラー: " + e.toString());
    ui.alert(e.toString()); 
  }
}

/**
 * 2. 予約送信 (日+時を合体させてグループ化)
 */
function createNotificationSchedule() {
  const ui = SpreadsheetApp.getUi();
  const { schedules, errors } = getSchedulesFromSheet();

  if (errors.length > 0) {
    ui.alert("入力エラーがあります:\n" + errors.join("\n"));
    return;
  }

  if (Object.keys(schedules).length === 0) {
    ui.alert("送信対象が選択されていないか、日時が未入力です。");
    return;
  }

  // --- 10分前チェック ---
  const now = new Date();
  const tenMinutesLater = new Date(now.getTime() + 10 * 60 * 1000);

  for (let timeKey in schedules) {
    const scheduledTime = new Date(timeKey);
    if (scheduledTime < tenMinutesLater) {
      ui.alert("エラー：現在から10分以内の日時は設定できません。\n設定時間: " + timeKey);
      return;
    }
  }

  const confirm = ui.alert('予約登録確認', Object.keys(schedules).length + ' 件のスケジュールをAWSに登録しますか？', ui.ButtonSet.YES_NO);
  if (confirm !== ui.Button.YES) return;

  const region = "ap-northeast-1";
  let successCount = 0;

  try {
    for (let timeKey in schedules) {
      const scheduledTime = new Date(timeKey);
      const payload = { "notifications": schedules[timeKey] };
      const dateStr = Utilities.formatDate(scheduledTime, "JST", "yyyy-MM-dd'T'HH:mm:ss");
      const scheduleName = "kusa-Push-" + Utilities.formatDate(new Date(), "JST", "yyyyMMdd-HHmmss") + "-" + successCount;

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

      callAwsScheduler(awsPayload, region);
      saveLogToSheet(payload, "予約成功: " + scheduleName + " (実行予定: " + timeKey + ")");
      successCount++;
    }

    // ★ すべての予約登録が成功したのでチェックボックスを外す
    clearCheckboxes();
    
    ui.alert('完了：' + successCount + ' 件の予約を登録しました。');
  } catch (e) { 
    saveLogToSheet({}, "AWS予約エラー: " + e.message);
    ui.alert("AWS予約エラー: " + e.message); 
  }
}

/**
 * 【追加】チェックボックスをすべてOFFにする
 */
function clearCheckboxes() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return; // データがない場合は何もしない
  
  // D列（4列目）の2行目から最後までの範囲を FALSE（OFF）にする
  sheet.getRange(2, 4, lastRow - 1, 1).setValue(false);
}

/**
 * 共通：シートから情報を取得
 */
function getSchedulesFromSheet() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const data = sheet.getDataRange().getValues();
  const schedules = {}; 
  const errors = [];

  for (let i = 1; i < data.length; i++) {
    const [lang, title, body, isTarget, datePart, timePart] = data[i];

    if (isTarget === true) {
      if (!(datePart instanceof Date) || timePart === "") {
        errors.push((i + 1) + "行目：日付または時刻が正しくありません。");
        continue;
      }

      const scheduledTime = new Date(datePart);
      const timeStr = Utilities.formatDate(new Date(timePart), "JST", "HH:mm");
      const [hours, minutes] = timeStr.split(":").map(Number);
      scheduledTime.setHours(hours, minutes, 0, 0);

      const timeKey = Utilities.formatDate(scheduledTime, "JST", "yyyy/MM/dd HH:mm:00");
      
      if (!schedules[timeKey]) {
        schedules[timeKey] = [];
      }

      schedules[timeKey].push({
        "lang": lang.toString().trim(),
        "title": title.toString(),
        "body": body.toString()
      });
    }
  }
  return { schedules, errors };
}

/**
 * 共通：AWS署名計算
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
  const options = { method: method, contentType: 'application/json', headers: { 'Authorization': authHeader, 'X-Amz-Date': datetime }, payload: payloadBytes, muteHttpExceptions: true };
  const response = UrlFetchApp.fetch('https://' + host + canonicalUri, options);
  if (response.getResponseCode() !== 200) throw new Error("AWS Error: " + response.getContentText());
}

/**
 * 共通：履歴保存
 * 実行者のメールアドレスを記録するように修正
 */
function saveLogToSheet(payload, result) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const logSheet = ss.getSheetByName("送信履歴") || ss.insertSheet("送信履歴");
  
  // 実行したユーザーのメールアドレスを取得
  const userEmail = Session.getActiveUser().getEmail();
  
  // 列の構成を [日時, 実行者, ペイロード, 結果] に変更
  logSheet.appendRow([
    Utilities.formatDate(new Date(), "JST", "yyyy/MM/dd HH:mm:ss"), 
    userEmail, // ★ ここに追加
    JSON.stringify(payload), 
    result
  ]);
}
function bytesToHex_(bytes) {
  return bytes.map(function(byte) { return ('0' + (byte & 0xFF).toString(16)).slice(-2); }).join('');
}
