function step10shipupdateSheet() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName("Shipped");
  var itemMasterSheet = spreadsheet.getSheetByName("Item Master");

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
  var shipToIndex = headers.indexOf("shipTo"); // Add this line to find the shipTo column index

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
    "Warehouse",
    "customername",
    "customercity",
    "customerphone",
    "customerEmail",
  ]);

  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var sku = row[skuIndex];
    var mskuIndexInMaster = skuColumn.indexOf(sku);
    var packSize = "";
    var brand = "";
    var msku = "";

    if (mskuIndexInMaster !== -1) {
      packSize = packSizeColumn[mskuIndexInMaster];
      brand = brandColumn[mskuIndexInMaster];
      msku = mskuColumn[mskuIndexInMaster];
    }

    // Extract customer name, city, phone, and email from shipTo
    var shipTo = row[shipToIndex];
    var customername = "";
    var customercity = "";
    var customerphone = "";
    var customerEmail = "";

    if (shipTo) {
      // Split shipTo data by comma
      var shipToData = shipTo.split(",");

      for (var j = 0; j < shipToData.length; j++) {
        var keyValue = shipToData[j].trim().split("=");

        if (keyValue.length === 2) {
          var key = keyValue[0].trim();
          var value = keyValue[1].trim();

          if (key === "name") {
            customername = value;
          } else if (key === "city") {
            customercity = value;
          } else if (key === "phone") {
            customerphone = value;
          } else if (key === "Email") {
            customerEmail = value;
          }
        }
      }
    }

    // Rest of your code remains the same

    var newRow = [
      // ...
      customername,
      customercity,
      customerphone,
      customerEmail,
    ];

    newData.push(newRow);
  }

  // Clear the existing data and update the sheet with the new data
  sheet.clearContents();
  sheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
}
