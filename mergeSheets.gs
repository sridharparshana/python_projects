function mergeSheets() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();
  var destinationSheet = ss.getSheetByName("Destination"); //change this to the name of the sheet you want to merge all sheets to
  
  var destinationRange = destinationSheet.getRange(destinationSheet.getLastRow() + 1, 1); //set the starting cell to the first empty row in the destination sheet
  
  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getName() !== destinationSheet.getName()) {
      var sourceRange = sheets[i].getDataRange();
      var sourceValues = sourceRange.getValues();
      destinationRange.offset(0, 0, sourceValues.length, sourceValues[0].length).setValues(sourceValues);
      destinationRange = destinationRange.offset(sourceValues.length, 0); //set the starting cell for the next sheet to the first empty row below the current data
    }
  }
}
