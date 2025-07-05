function step4_distribute() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName("Unshipped orders");
  var usStockSheet = spreadsheet.getSheetByName("US Stock orders");
  var toProcureSheet = spreadsheet.getSheetByName("To procure");

  var dataRange = sheet.getDataRange();
  var values = dataRange.getValues();
  var headers = values[0];

  var orderIDIndex = headers.indexOf("order-id");
  var packsizeQuantityIndex = headers.indexOf("packsize-quantity");
  var belmontStockIndex = headers.indexOf("belmont-stock");
  var richmondStockIndex = headers.indexOf("richmond-stock");

  var uniqueOrders = {};

  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var orderID = row[orderIDIndex];
    var packsizeQuantity = row[packsizeQuantityIndex];
    var belmontStock = row[belmontStockIndex];
    var richmondStock = row[richmondStockIndex];

    if (!uniqueOrders[orderID]) {
      uniqueOrders[orderID] = {
        inStockCount: 0,
        totalCount: 0
      };
    }

    uniqueOrders[orderID].totalCount++;

    if (packsizeQuantity <= belmontStock || packsizeQuantity <= richmondStock) {
      uniqueOrders[orderID].inStockCount++;
    }
  }

  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var orderID = row[orderIDIndex];

    if (uniqueOrders[orderID].inStockCount === uniqueOrders[orderID].totalCount) {
      usStockSheet.appendRow(row);
    } else {
      toProcureSheet.appendRow(row);
    }
  }

  // Clear the rows in the "Unshipped orders" sheet
  sheet.clearContents();
}
