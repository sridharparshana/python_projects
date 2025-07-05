function step5checkshippedorders() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var usStockOrdersSheet = spreadsheet.getSheetByName("Unshipped Profitability");
  var unshippedOrdersSheet = spreadsheet.getSheetByName("Unshipped");
  var shippedSheet = spreadsheet.getSheetByName("Shipped orders profitability");

  var usStockOrdersData = usStockOrdersSheet.getDataRange().getValues();
  var unshippedOrdersData = unshippedOrdersSheet.getDataRange().getValues();

  var unshippedOrderIds = unshippedOrdersData.map(function(row) { return row[0]; }); // Assuming Order ID is in column A (column index 0)

  var movedOrders = []; // Array to store the moved order IDs

  // Iterate over each row in the US Stock orders sheet
  for (var j = usStockOrdersData.length - 1; j > 0; j--) {
    var orderRow = usStockOrdersData[j]; // Get the entire row
    var orderId = orderRow[0]; // Assuming Order ID is in column A (column index 0)

    if (!unshippedOrderIds.includes(orderId) && !movedOrders.includes(orderId)) {
      // Move all rows with matching Order ID to the Shipped sheet
      var movedRows = []; // Array to store the rows to be moved

      for (var k = j; k > 0; k--) {
        if (usStockOrdersData[k][0] === orderId) {
          movedRows.unshift(usStockOrdersData[k]); // Add the row to the beginning of the movedRows array (to maintain order)
        }
      }

      // Append all moved rows to the Shipped sheet
      for (var l = 0; l < movedRows.length; l++) {
        shippedSheet.appendRow(movedRows[l]);
      }

      // Delete the matching rows from the US Stock orders sheet
      for (var m = usStockOrdersData.length - 1; m > 0; m--) {
        if (usStockOrdersData[m][0] === orderId) {
          usStockOrdersSheet.deleteRow(m + 1);
        }
      }

      movedOrders.push(orderId); // Add the moved order ID to the movedOrders array
    }
  }
}
