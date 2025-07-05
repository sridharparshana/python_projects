function step1_mskuupdate() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName("Unshipped orders");
  var itemMasterSheet = spreadsheet.getSheetByName("Item Master");
  var belmontStockSheet = spreadsheet.getSheetByName("Belmont stock");
  var richmondStockSheet = spreadsheet.getSheetByName("Richmond stock");
  var zonessheet = spreadsheet.getSheetByName("Zones");

  // Fetch all data at once
  var dataRange = sheet.getDataRange();
  var values = dataRange.getValues();
  var headers = values[0];
  
  var itemMasterData = itemMasterSheet.getRange("A:G").getValues();
  var belmontStockValues = belmontStockSheet.getRange("B:C").getValues();
  var richmondStockValues = richmondStockSheet.getRange("B:C").getValues();
  var zonesData = zonessheet.getRange("A:C").getValues();

  // Create maps for quick lookup
  var itemMasterMap = createMap(itemMasterData, 0);
  var belmontStockMap = createMap(belmontStockValues, 0);
  var richmondStockMap = createMap(richmondStockValues, 0);
  var zonesMap = createMap(zonesData, 0);

  var newData = [];
  newData.push(["order-id", "order-item-id", "purchase-date", "promise-date", "sku", "product-name", "quantity-purchased", "msku", "packsize", "brand", "packsize-quantity", "belmont-stock", "richmond-stock", "Channel", "ship-state", "Region"]);

  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var sku = row[headers.indexOf("sku")];
    var itemMasterRow = itemMasterMap[sku];
    var msku = itemMasterRow ? itemMasterRow[3] : "";
    var packSize = itemMasterRow ? itemMasterRow[4] : 0;
    var brand = itemMasterRow ? itemMasterRow[6] : "";
    var packSizeQuantity = packSize * row[headers.indexOf("quantity-purchased")];
    var belmontStock = belmontStockMap[msku] ? belmontStockMap[msku][1] : "";
    var richmondStock = richmondStockMap[msku] ? richmondStockMap[msku][1] : "";
    var region = zonesMap[row[headers.indexOf("ship-state")]] ? zonesMap[row[headers.indexOf("ship-state")]][2] : "";

    var newRow = [
      row[headers.indexOf("order-id")],
      row[headers.indexOf("order-item-id")],
      row[headers.indexOf("purchase-date")].substring(0, 10),
      row[headers.indexOf("promise-date")].substring(0, 10),
      sku,
      row[headers.indexOf("product-name")],
      row[headers.indexOf("quantity-purchased")],
      msku,
      packSize,
      brand,
      packSizeQuantity,
      belmontStock,
      richmondStock,
      row[headers.indexOf("Channel")],
      row[headers.indexOf("ship-state")],
      region
    ];
    
    newData.push(newRow);
  }
  
  sheet.clearContents();
  sheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
}

function createMap(data, keyIndex) {
  var map = {};
  for (var i = 0; i < data.length; i++) {
    map[data[i][keyIndex]] = data[i];
  }
  return map;
}
