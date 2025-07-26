function step1processData() {
  var apiKey = 'f6ddc41804a2466f93418aa777624837';
  var apiSecret = 'e10a81745b844d3084a51580102f6e20';
  var url = 'https://ssapi.shipstation.com/orders?orderStatus=awaiting_shipment&pageSize=500';
  var headers = { 'Authorization': 'Basic ' + Utilities.base64Encode(apiKey + ':' + apiSecret) };
  var options = { 'headers': headers, 'method': 'GET', 'muteHttpExceptions': true };

  var responseData = [];
  var nextPage = 1, totalPages = 1;

  while (nextPage <= totalPages) {
    var pageUrl = url + '&page=' + nextPage;
    var response = UrlFetchApp.fetch(pageUrl, options);
    var pageData = JSON.parse(response.getContentText());
    responseData = responseData.concat(pageData.orders);
    totalPages = pageData.pages;
    nextPage++;
  }

  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName('Unshipped orders') || spreadsheet.insertSheet('Unshipped orders');

  var itemMasterSheet = spreadsheet.getSheetByName("Item Master");
  var belmontStockSheet = spreadsheet.getSheetByName("Belmont stock");
  var richmondStockSheet = spreadsheet.getSheetByName("Richmond stock");
  var haridwarStockSheet = spreadsheet.getSheetByName("Haridwar stock");
  var hyderabadStockSheet = spreadsheet.getSheetByName("Hyderabad stock");

  var itemMasterData = itemMasterSheet.getRange("A:J").getValues();
  var mskuList = itemMasterData.map(r => r[3]); // Column D = MSKU
  var skuMap = {}; // msku => { packSize, brand, price }

  for (var i = 0; i < itemMasterData.length; i++) {
    var msku = itemMasterData[i][3]; // Column D
    if (!msku) continue;
    skuMap[msku] = {
      packSize: Number(itemMasterData[i][4]) || 1, // E
      brand: itemMasterData[i][6] || "", // G
      price: Number(itemMasterData[i][7]) || 0 // H
    };
  }

  var belmontStockData = getStockData(belmontStockSheet, 1, 2);
  var richmondStockData = getStockData(richmondStockSheet, 1, 2);
  var haridwarStockData = getStockData(haridwarStockSheet, 1, 2);
  var hyderabadStockData = getStockData(hyderabadStockSheet, 1, 2);

  var output = [];
  output.push([
    "orderNumber", "lineItemKey", "orderDate", "shipByDate",
    "sku", "msku", "productName", "quantity", "packSize", "totalQuantity",
    "brand", "price", "totalCost", "unitPrice", "salePrice",
    "belmontStock", "richmondStock", "haridwarStock", "hyderabadStock",
    "state", "country"
  ]);

  responseData.forEach(order => {
    var channel = order.advancedOptions?.storeId?.toString() || "";
    if (channel !== "2022985") return; // Only process this store/channel

    var orderNumber = order.orderNumber;
    var orderDate = formatDate(order.orderDate, spreadsheet);
    var shipByDate = formatDate(order.shipByDate, spreadsheet);
    var shipTo = order.shipTo || {};
    var state = shipTo.state || "";
    var country = shipTo.country || "";

    order.items.forEach(item => {
      var sku = item.sku;
      var msku = sku;

      // Match with Item Master MSKU (column D)
      if (!sku || !skuMap[msku]) return;

      var details = skuMap[msku];
      var quantity = item.quantity;
      var packSize = details.packSize;
      var totalQuantity = quantity * packSize;
      var price = details.price;
      var totalCost = totalQuantity * price;
      var salePrice = totalQuantity * item.unitPrice;

      var row = [
        orderNumber,
        item.lineItemKey,
        orderDate,
        shipByDate,
        sku,
        msku,
        item.name,
        quantity,
        packSize,
        totalQuantity,
        details.brand,
        price,
        totalCost,
        item.unitPrice,
        salePrice,
        belmontStockData[msku] || "",
        richmondStockData[msku] || "",
        haridwarStockData[msku] || "",
        hyderabadStockData[msku] || "",
        state,
        country
      ];
      output.push(row);
    });
  });

  sheet.clearContents();
  sheet.getRange(1, 1, output.length, output[0].length).setValues(output);
}

// Helper: get stock data from sheet as { msku: stock }
function getStockData(sheet, mskuIndex, stockIndex) {
  var data = {};
  var values = sheet.getDataRange().getValues();
  for (var i = 0; i < values.length; i++) {
    var msku = values[i][mskuIndex];
    if (!msku) continue;
    data[msku] = values[i][stockIndex];
  }
  return data;
}

// Helper: format dates
function formatDate(input, spreadsheet) {
  if (!input) return "";
  return Utilities.formatDate(new Date(input), spreadsheet.getSpreadsheetTimeZone(), "yyyy-MM-dd");
}
