function step4PivotTable() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var unshippedSheet = spreadsheet.getSheetByName("Unshipped");
  var pickProductsSheet = spreadsheet.getSheetByName("Pick products");

  // Clear existing data in Pick products sheet
  pickProductsSheet.clearContents();

  // Get data range from Unshipped sheet
  var dataRange = unshippedSheet.getDataRange();
  var dataValues = dataRange.getValues();
  var headers = dataValues[0];

  // Find the column index for name, brand, and totalQuantity
  var nameIndex = headers.indexOf("name");
  var brandIndex = headers.indexOf("brand");
  var totalQuantityIndex = headers.indexOf("totalQuantity");

  // Create an object to store the aggregated quantities for each name and brand
  var quantities = {};

  // Process each row of data and aggregate quantities by name and brand
  for (var i = 1; i < dataValues.length; i++) {
    var row = dataValues[i];
    var name = row[nameIndex];
    var brand = row[brandIndex];
    var totalQuantity = row[totalQuantityIndex];

    if (name) {
      var key = name;

      if (brand) {
        key += "_" + brand;
      } else {
        brand = ""; // Set empty brand for sorting purposes
      }

      if (!quantities[key]) {
        quantities[key] = 0;
      }

      quantities[key] += totalQuantity;
    }
  }

  // Prepare data for the pivot table
  var pivotData = [];
  pivotData.push(["Name", "Brand", "Total Quantity"]);

  for (var key in quantities) {
    var parts = key.split("_");
    var name = parts[0];
    var brand = parts[1];
    var totalQuantity = quantities[key];
    pivotData.push([name, brand, totalQuantity]);
  }

  // Sort the pivot data based on the Brand name (column index 1), excluding the header row
  pivotData.sort(function(a, b) {
    if (a[1] === undefined && b[1] !== undefined) return 1; // Empty brand should come last
    if (a[1] !== undefined && b[1] === undefined) return -1; // Empty brand should come last

    // Exclude sorting of the header row
    if (a[0] === "Name" || a[0] === "Brand" || a[0] === "Total Quantity") return -1;
    if (b[0] === "Name" || b[0] === "Brand" || b[0] === "Total Quantity") return 1;

    if (a[1] === b[1]) return 0;
    return a[1].localeCompare(b[1]);
  });

  // Set the pivot table data in the Pick products sheet starting from row 2
  var pivotRange = pickProductsSheet.getRange(2, 1, pivotData.length, pivotData[0].length);
  pivotRange.setValues(pivotData);

  // Set the header row in the first row of the Pick products sheet
  pickProductsSheet.getRange(1, 1, 1, pivotData[0].length).setValues([["Name", "Brand", "Total Quantity"]]);

  // Get the last row and column of the pivot table data
  var lastRow = pivotData.length + 1;
  var lastColumn = pivotData[0].length;

  // Create the pivot table using formulas in row 1 of the Pick products sheet
  var pivotTableRange = pickProductsSheet.getRange("A1:C" + lastRow);
  var pivotTableFormula =
    "=QUERY(" +
    pivotTableRange.getA1Notation() +
    ', "SELECT A, B, SUM(C) WHERE A <> \'Name\' GROUP BY A, B LABEL A \'Name\', B \'Brand\', SUM(C) \'Total Quantity\'", 1)';
  pickProductsSheet.getRange(1, 1).setValue(pivotTableFormula);

  // Resize the columns to fit the data
  pickProductsSheet.autoResizeColumns(1, lastColumn);
}
