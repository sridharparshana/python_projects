function updateStock() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var usStockOrdersSheet = ss.getSheetByName("Unshipped");
  var belmontStockSheet = ss.getSheetByName("Belmont stock");
  var richmondStockSheet = ss.getSheetByName("Richmond stock");
  var toProcureSheet = ss.getSheetByName("Partial orders");
  
  // Get data from "Belmont stock" sheet
  var belmontStockData = belmontStockSheet.getRange("B:C").getValues();
  
  // Get data from "Richmond stock" sheet
  var richmondStockData = richmondStockSheet.getRange("B:C").getValues();
  
  // Get data from "To procure" sheet
  var toProcureData = toProcureSheet.getDataRange().getValues();
  
  // Find column index of "msku" and "belmont-stock" in "To procure" sheet
  var toProcureHeaders = toProcureData[0];
  var mskuColumnIndex = toProcureHeaders.indexOf("msku");
  var belmontStockColumnIndex = toProcureHeaders.indexOf("Belmont stock");
  
  // Find column index of "msku" and "richmond-stock" in "To procure" sheet
  var richmondStockColumnIndex = toProcureHeaders.indexOf("Richmond stock");
  
  // Update "belmont-stock" and "richmond-stock" quantities
  for (var i = 1; i < toProcureData.length; i++) {
    var msku = toProcureData[i][mskuColumnIndex];
    
    // Update "belmont-stock" quantity
    for (var j = 0; j < belmontStockData.length; j++) {
      if (belmontStockData[j][0] == msku) {
        toProcureData[i][belmontStockColumnIndex] = belmontStockData[j][1];
        break;
      }
    }
    
    // Update "richmond-stock" quantity
    for (var k = 0; k < richmondStockData.length; k++) {
      if (richmondStockData[k][0] == msku) {
        toProcureData[i][richmondStockColumnIndex] = richmondStockData[k][1];
        break;
      }
    }
  }
  
  // Update "To procure" sheet with the modified data
  toProcureSheet.getRange(1, 1, toProcureData.length, toProcureData[0].length).setValues(toProcureData);
}
