function step2UnshipupdateSheet() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName("Unshipped");
  var itemMasterSheet = spreadsheet.getSheetByName("Item Master");
  var belmontStockSheet = spreadsheet.getSheetByName("Belmont stock");
  var richmondStockSheet = spreadsheet.getSheetByName("Richmond stock");
  var haridwarStockSheet = spreadsheet.getSheetByName("Haridwar stock");
  var hyderabadStockSheet = spreadsheet.getSheetByName("Hyderabad stock");
  var pricemasterSheet = spreadsheet.getSheetByName("Price master");

  var dataRange = sheet.getDataRange();
  var values = dataRange.getValues();
  var headers = values[0];

  // Find the column indexes for the required columns
  var orderIndex = headers.indexOf("orderNumber");
  var orderItemIndex = headers.indexOf("lineItemKey");
  var purchaseDateIndex = headers.indexOf("orderDate");
  var promiseDateIndex = headers.indexOf("shipDate");
  var skuIndex = headers.indexOf("sku");
  var productNameIndex = headers.indexOf("name");
  var quantityIndex = headers.indexOf("quantity");
  var unitPriceIndex = headers.indexOf("unitPrice");
  var taxAmountIndex = headers.indexOf("taxAmount");
  var advancedOptionsIndex = headers.indexOf("advancedOptions");

  // Get the item master data
  var itemMasterData = itemMasterSheet.getRange("A:J").getValues();
  var skuColumn = itemMasterData.map(function(row) {
    return row[0];
  });
  var mskuColumn = itemMasterData.map(function(row) {
    return row[2];
  });
  var packSizeColumn = itemMasterData.map(function(row) {
    return row[3];
  });
  var brandColumn = itemMasterData.map(function(row) {
    return row[7];
  });

  var pricemasterData = pricemasterSheet.getRange("A:G").getValues();
  var priceColumn = pricemasterData.map(function(row) {
    return row[6];
  });

  // Create a new array to store the filtered and updated data
  var newData = [];
  newData.push([
    "orderNumber",
    "lineItemKey",
    "orderDate",
    "shipDate",
    "sku",
    "name",
    "quantity",
    "msku",
    "packSize",
    "brand",
    "totalQuantity",
    "Channel",
    "unitPrice",
    "taxAmount",
    "Belmont stock",
    "Richmond stock",
    "Haridwar stock",
    "Hyderabad stock", 
    "disc. purchase cost",
    "total cost", 
    "Warehouse"
  ]);

  // Create objects to store stock data for faster lookup
  var belmontStockData = getStockData(belmontStockSheet, 1, 2);
  var richmondStockData = getStockData(richmondStockSheet, 1, 2);
  var haridwarStockData = getStockData(haridwarStockSheet, 0, 4);
  var hyderabadStockData = getStockData(hyderabadStockSheet, 0, 4);

  // Process each row of data
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var sku = row[skuIndex];
    var msku = "";
    var belmontStock = "";
    var richmondStock = "";
    var haridwarStock = "";
    var hyderabadStock = "";

    var skuIndexInMaster = skuColumn.indexOf(sku);
    if (skuIndexInMaster !== -1) {
      msku = mskuColumn[skuIndexInMaster];

      // Lookup Belmont stock
      if (belmontStockData.hasOwnProperty(msku)) {
        belmontStock = belmontStockData[msku];
      }

      // Lookup Richmond stock
      if (richmondStockData.hasOwnProperty(msku)) {
        richmondStock = richmondStockData[msku];
      }

      // Lookup Haridwar stock
      if (haridwarStockData.hasOwnProperty(msku)) {
        haridwarStock = haridwarStockData[msku];
      }

      // Lookup Hyderabad stock
      if (hyderabadStockData.hasOwnProperty(msku)) {
        hyderabadStock = hyderabadStockData[msku];
      }
    }

    // Get the pack size and brand
    var packSize = "";
    var brand = "";
    if (skuIndexInMaster !== -1) {
      packSize = packSizeColumn[skuIndexInMaster];
      brand = brandColumn[skuIndexInMaster];
    }

    // Convert values to strings and extract the left 10 characters
    var newValueD = row[purchaseDateIndex]
      ? Utilities.formatDate(
          row[purchaseDateIndex],
          spreadsheet.getSpreadsheetTimeZone(),
          "yyyy-MM-dd"
        )
      : "";
    var newValueH = row[promiseDateIndex]
      ? Utilities.formatDate(
          row[promiseDateIndex],
          spreadsheet.getSpreadsheetTimeZone(),
          "yyyy-MM-dd"
        )
      : "";

    // Calculate total quantity based on quantity and pack size
    var quantity = row[quantityIndex];
    var totalQuantity = quantity * packSize;

    // Get the price based on SKU
    var price = "";
    if (skuIndexInMaster !== -1) {
      price = priceColumn[skuIndexInMaster];
    }

    // Calculate the total cost
    var totalCost = totalQuantity * price;

    // Get Channel and Warehouse from advancedOptions
    var advancedOptions = row[advancedOptionsIndex];
    var channel = "";
    var warehouse = "";
    if (advancedOptions) {
      var storeIdMatch = advancedOptions.match(/storeId=(\d+)/);
      var warehouseIdMatch = advancedOptions.match(/warehouseId=(\d+)/);
      channel = storeIdMatch ? storeIdMatch[1] : "";
      warehouse = warehouseIdMatch ? warehouseIdMatch[1] : "";
    }

    var newRow = [
      row[orderIndex],
      row[orderItemIndex],
      newValueD,
      newValueH,
      sku,
      row[productNameIndex],
      quantity,
      msku,
      packSize,
      brand,
      totalQuantity,
      channel,
      row[unitPriceIndex],
      row[taxAmountIndex],
      belmontStock,
      richmondStock,
      haridwarStock,
      hyderabadStock,
      price,
      totalCost,
      warehouse
    ];
    newData.push(newRow);
  }

  // Clear the existing data and update the sheet with the new data
  sheet.clearContents();
  sheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
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
