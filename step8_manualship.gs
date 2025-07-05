function step8manualship() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName("Procured");
  const shippedSheet = spreadsheet.getSheetByName("Shipped");

  // Get the range of data in the To procure sheet
  const dataRange = sheet.getDataRange();
  const data = dataRange.getValues();

  // Create an array to store rows to be moved
  const rowsToMoveToProcured = [];

  // Loop through the rows of data starting from the second row (excluding the header row)
  for (let i = 1; i < data.length; i++) {
    const status = data[i][15];

    // Check if the status is "Shipped"
    if (status === 'Shipped') {
      // Add the row to the rowsToMoveToProcured array
      rowsToMoveToProcured.push(data[i]);
    }
  }

  // Move rows to the Procured sheet
  if (rowsToMoveToProcured.length > 0) {
    const lastProcuredRow = shippedSheet.getLastRow();
    const procuredRange = shippedSheet.getRange(lastProcuredRow + 1, 1, rowsToMoveToProcured.length, data[0].length);
    procuredRange.setValues(rowsToMoveToProcured);

    // Delete the moved rows from the To procure sheet
    const numRowsToDelete = rowsToMoveToProcured.length;
    sheet.deleteRows(2, numRowsToDelete);
  }
}
