function step3Distribute() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName("Unshipped");
  var partialOrdersSheet = spreadsheet.getSheetByName("Partial orders");

  // Clear the existing data in Partial orders sheet
  partialOrdersSheet.clearContents();

  var dataRange = sheet.getDataRange();
  var values = dataRange.getValues();
  var headers = values[0];

  // Find the column indexes for the required columns
  var orderNumberIndex = headers.indexOf("orderNumber");
  var belmontStockIndex = headers.indexOf("Belmont stock");
  var totalQuantityIndex = headers.indexOf("totalQuantity");
  var skuIndex = headers.indexOf("sku");

  // Create an object to store unique order numbers and their status
  var orderStatus = {};

  // Process each row of data
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var orderNumber = row[orderNumberIndex];
    var belmontStock = row[belmontStockIndex];
    var totalQuantity = row[totalQuantityIndex];
    var sku = row[skuIndex];

    // Skip rows with no SKU
    if (!sku) {
      continue;
    }

    // Update the order status
    if (!orderStatus[orderNumber]) {
      orderStatus[orderNumber] = {
        hasPartialOrder: false,
        allPartial: true
      };
    }

    // Check if the current item has total quantity less than Belmont stock
    if (totalQuantity >= belmontStock) {
      orderStatus[orderNumber].allPartial = false;
    }

    // Check if the current item has total quantity equal to Belmont stock
    if (totalQuantity === belmontStock) {
      orderStatus[orderNumber].hasPartialOrder = true;
    }
  }

  // Create new arrays to store the filtered data for Unshipped and Partial orders sheets
  var newDataUnshipped = [];
  var newDataPartialOrders = [];
  newDataUnshipped.push(headers);
  newDataPartialOrders.push(headers);

  // Process each row of data again to distribute orders
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var orderNumber = row[orderNumberIndex];
    var hasPartialOrder = orderStatus[orderNumber].hasPartialOrder;
    var allPartial = orderStatus[orderNumber].allPartial;

    // Skip rows with no SKU
    if (!row[skuIndex]) {
      continue;
    }

    // Create a copy of the current row
    var newRow = row.slice();

    // Convert date values to strings
    for (var j = 2; j < newRow.length; j++) {
      if (newRow[j] instanceof Date) {
        newRow[j] = Utilities.formatDate(
          newRow[j],
          spreadsheet.getSpreadsheetTimeZone(),
          "yyyy-MM-dd"
        );
      }
    }

    // Distribute the rows based on the conditions
    if (hasPartialOrder || allPartial) {
      newDataUnshipped.push(newRow);
    } else {
      newDataPartialOrders.push(newRow);
    }
  }

  // Clear the existing data in Unshipped sheet and update with the new data
  sheet.clearContents();
  sheet.getRange(1, 1, newDataUnshipped.length, newDataUnshipped[0].length).setValues(newDataUnshipped);

  // Update the data in Partial orders sheet
  partialOrdersSheet.getRange(1, 1, newDataPartialOrders.length, newDataPartialOrders[0].length).setValues(newDataPartialOrders);
}
