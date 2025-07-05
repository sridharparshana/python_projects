function step7_status() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName("To procure");
  const procuredSheet = spreadsheet.getSheetByName("Procured");

  // Get the range of data in the To procure sheet
  const dataRange = sheet.getDataRange();
  const data = dataRange.getValues();

  // Create an array to store rows to be moved
  const rowsToMoveToProcured = [];

  // Loop through the rows of data starting from the second row (excluding the header row)
  for (let i = data.length - 1; i > 0; i--) {
    const status = data[i][15];

    if (status === 'Received') {
      // Add the row to the rowsToMoveToProcured array
      rowsToMoveToProcured.unshift(data[i]); // Prepend the row to maintain the original order
      sheet.deleteRow(i + 1); // Delete the row from the To procure sheet
    }
  }

  // Move rows to the Procured sheet
  if (rowsToMoveToProcured.length > 0) {
    const lastProcuredRow = procuredSheet.getLastRow();
    const procuredRange = procuredSheet.getRange(lastProcuredRow + 1, 1, rowsToMoveToProcured.length, data[0].length);
    procuredRange.setValues(rowsToMoveToProcured.reverse()); // Reverse the order to match the original

    // Optionally, you can sort the Procured sheet by a column to maintain a specific order
    // For example, if you want to sort by the first column (assuming it's a unique identifier)
    procuredSheet.getRange(2, 1, lastProcuredRow + rowsToMoveToProcured.length, data[0].length).sort({ column: 1 });
  }
}
