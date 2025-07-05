function step3_removeexisting() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var toProcureSheet = spreadsheet.getSheetByName("To procure");
  var usStockOrdersSheet = spreadsheet.getSheetByName("US Stock orders");
  var refundSheet = spreadsheet.getSheetByName("Refund");
  var unshippedOrdersSheet = spreadsheet.getSheetByName("Unshipped orders");

  var toProcureData = toProcureSheet.getDataRange().getValues();
  var usStockOrdersData = usStockOrdersSheet.getDataRange().getValues();
  var refundData = refundSheet.getDataRange().getValues();
  var unshippedOrdersData = unshippedOrdersSheet.getDataRange().getValues();

  var toProcureOrderIds = toProcureData.map(function(row) { return row[0]; }); // Assuming Order ID is in column A (column index 0)
  var usStockOrderIds = usStockOrdersData.map(function(row) { return row[0]; }); // Assuming Order ID is in column A (column index 0)
  var refundOrderIds = refundData.map(function(row) { return row[0]; }); // Assuming Order ID is in column A (column index 0)

  // Iterate over each row in the Unshipped orders sheet
  for (var i = unshippedOrdersData.length - 1; i > 0; i--) {
    var orderId = unshippedOrdersData[i][0]; // Assuming Order ID is in column A (column index 0)

    if (toProcureOrderIds.includes(orderId) || usStockOrderIds.includes(orderId) || refundOrderIds.includes(orderId)) {
      // Delete the row from the Unshipped orders sheet
      unshippedOrdersSheet.deleteRow(i + 1);
    }
  }
}
