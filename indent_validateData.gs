function validateData() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var hyderabadSheet = ss.getSheetByName("Hyderabad indent");
  var haridwarSheet = ss.getSheetByName("Haridwar indent");
  var historySheet = ss.getSheetByName("Indent history");
  var unshippedSheet = ss.getSheetByName("Unshipped");

  var hyderabadData = hyderabadSheet.getRange("C2:D" + hyderabadSheet.getLastRow()).getValues();
  var haridwarData = haridwarSheet.getRange("C2:D" + haridwarSheet.getLastRow()).getValues();
  var historyData = historySheet.getRange("C2:D" + historySheet.getLastRow()).getValues();
  var unshippedData = unshippedSheet.getRange("C2:D" + unshippedSheet.getLastRow()).getValues();

  var errors = [];

  for (var i = 0; i < hyderabadData.length; i++) {
    var orderNumber = hyderabadData[i][0];
    var lineItemKey = hyderabadData[i][1];

    if (orderNumber !== "" && lineItemKey !== "") {
      if (
        isCombinationExists(orderNumber, lineItemKey, haridwarData) ||
        isCombinationExists(orderNumber, lineItemKey, historyData)
      ) {
        errors.push(
          "OrderNumber " +
            orderNumber +
            " and LineItemKey " +
            lineItemKey +
            " combination already exists in Haridwar indent or Indent history."
        );
      } else if (!isCombinationExists(orderNumber, lineItemKey, unshippedData)) {
        errors.push(
          "OrderNumber " +
            orderNumber +
            " and LineItemKey " +
            lineItemKey +
            " combination is not found in Unshipped sheet."
        );
      }
    }
  }

  if (errors.length > 0) {
    var errorString = errors.join("\n");
    Logger.log(errorString);
    // You can choose to display the errors in a dialog box or handle them as per your requirement
    // For example, you can use the following line to show a dialog box with the errors
    // Browser.msgBox(errorString);
  } else {
    Logger.log("Data validation successful!");
    // You can perform additional actions if the data validation is successful
  }
}

function isCombinationExists(orderNumber, lineItemKey, data) {
  for (var i = 0; i < data.length; i++) {
    if (data[i][0] === orderNumber && data[i][1] === lineItemKey) {
      return true;
    }
  }
  return false;
}
