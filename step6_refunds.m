function step6_refunds() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var usStockOrdersSheet = spreadsheet.getSheetByName("US Stock orders");
  var toProcureSheet = spreadsheet.getSheetByName("To procure");
  var refundSheet = spreadsheet.getSheetByName("Refund");

  var today = new Date();
  today.setHours(0, 0, 0, 0); // Set hours, minutes, seconds, and milliseconds to zero

  var usStockOrdersData = usStockOrdersSheet.getDataRange().getValues();
  var toProcureData = toProcureSheet.getDataRange().getValues();

  // Check the promise date in the US Stock orders sheet
  for (var i = usStockOrdersData.length - 1; i > 0; i--) {
    var promiseDate = new Date(usStockOrdersData[i][3]); // Assuming promise date is in column D (column index 3)
    promiseDate.setHours(0, 0, 0, 0); // Set hours, minutes, seconds, and milliseconds to zero

    if (promiseDate <= today) {
      // Move the row to the Refund sheet
      refundSheet.appendRow(usStockOrdersData[i]);
      usStockOrdersSheet.deleteRow(i + 1);
    }
  }

  // Check the promise date in the To procure sheet
  for (var j = toProcureData.length - 1; j > 0; j--) {
    var promiseDate = new Date(toProcureData[j][3]); // Assuming promise date is in column D (column index 3)
    promiseDate.setHours(0, 0, 0, 0); // Set hours, minutes, seconds, and milliseconds to zero

    if (promiseDate <= today) {
      // Move the row to the Refund sheet
      refundSheet.appendRow(toProcureData[j]);
      toProcureSheet.deleteRow(j + 1);
    }
  }
}
