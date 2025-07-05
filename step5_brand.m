function step5_brand() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var toProcureSheet = spreadsheet.getSheetByName("To procure");
  var brandResponsibilitySheet = spreadsheet.getSheetByName("Brand Responsibility");

  // Get the data from both sheets
  var toProcureData = toProcureSheet.getDataRange().getValues();
  var brandResponsibilityData = brandResponsibilitySheet.getDataRange().getValues();

  // Convert brand responsibility data into a lookup object for faster searching
  var brandMap = {};
  for (var j = 0; j < brandResponsibilityData.length; j++) {
    brandMap[brandResponsibilityData[j][0]] = brandResponsibilityData[j][1]; // Key: Brand Name, Value: Responsibility
  }

  // Iterate over each row in the "To procure" sheet
  for (var i = 1; i < toProcureData.length; i++) {
    var brandName = toProcureData[i][7]; // Column J (index 9)
    var brandResponsibilityCell = toProcureData[i][14]; // Column O (index 14)

    // Skip rows where column O already has a value
    if (brandResponsibilityCell !== "" && brandResponsibilityCell !== null && brandResponsibilityCell !== 0) {
      continue;
    }

    // Get the brand responsibility value from the map
    var brandResponsibilityValue = brandMap[brandName] || "";

    // Update column O (index 15 in 1-based range)
    toProcureSheet.getRange(i + 1, 15).setValue(brandResponsibilityValue);
  }
}
