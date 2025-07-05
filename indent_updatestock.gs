function updatestock() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var belmontStockSheet = spreadsheet.getSheetByName("Belmont stock");
  var richmondStockSheet = spreadsheet.getSheetByName("Richmond stock");
  var haridwarStockSheet = spreadsheet.getSheetByName("Haridwar stock");
  var hyderabadStockSheet = spreadsheet.getSheetByName("Hyderabad stock");

  // Create objects to store stock data for faster lookup
  var belmontStockData = getStockData(belmontStockSheet, 1, 2);
  var richmondStockData = getStockData(richmondStockSheet, 1, 2);
  var haridwarStockData = getStockData(haridwarStockSheet, 0, 4);
  var hyderabadStockData = getStockData(hyderabadStockSheet, 0, 4);

  // Retrieve the Unshipped sheet data
  var sheet = spreadsheet.getSheetByName("Unshipped");
  var dataRange = sheet.getDataRange();
  var values = dataRange.getValues();
  var headers = values[0];

  // Find the column indexes for the required columns
  var skuIndex = headers.indexOf("sku");
  var productNameIndex = headers.indexOf("name");
  var mskuIndex = headers.indexOf("msku");
  var belmontStockIndex = headers.indexOf("Belmont stock");
  var richmondStockIndex = headers.indexOf("Richmond stock");
  var haridwarStockIndex = headers.indexOf("Haridwar stock");
  var hyderabadStockIndex = headers.indexOf("Hyderabad stock");

  // Process each row of data
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var sku = row[skuIndex];
    var msku = row[mskuIndex];

    // Lookup Belmont stock
    if (belmontStockData.hasOwnProperty(msku)) {
      sheet.getRange(i + 1, belmontStockIndex + 1).setValue(belmontStockData[msku]);
    }

    // Lookup Richmond stock
    if (richmondStockData.hasOwnProperty(msku)) {
      sheet.getRange(i + 1, richmondStockIndex + 1).setValue(richmondStockData[msku]);
    }

    // Lookup Haridwar stock
    if (haridwarStockData.hasOwnProperty(msku)) {
      sheet.getRange(i + 1, haridwarStockIndex + 1).setValue(haridwarStockData[msku]);
    }

    // Lookup Hyderabad stock
    if (hyderabadStockData.hasOwnProperty(msku)) {
      sheet.getRange(i + 1, hyderabadStockIndex + 1).setValue(hyderabadStockData[msku]);
    }
  }
}

// Helper function to retrieve and store stock data in an object
function getStockData(stockSheet, mskuColumnIndex, stockColumnIndex) {
  var stockData = {};
  var stockValues = stockSheet.getDataRange().getValues();
  for (var i = 0; i < stockValues.length; i++) {
    var msku = stockValues[i][mskuColumnIndex];
    var stock = stockValues[i][stockColumnIndex];
    stockData[msku] = stock;
  }
  return stockData;
}
