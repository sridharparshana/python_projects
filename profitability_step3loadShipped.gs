function step3loadShipped() {
  var apiKey = '995c95a21dd74d68a3565bc5cb7f3e36';
  var apiSecret = 'f8687237ce9f47ba886568b2ccfe0752';

  var startDate = new Date();
  startDate.setFullYear(2023,9, 25); // June is represented as 5 since the month index starts from 0
  startDate.setHours(0, 0, 0, 0);

  var endDate = new Date();

  var url = 'https://ssapi.shipstation.com/orders?orderStatus=shipped&pageSize=500&orderDateStart=' + formatDate(startDate) + '&orderDateEnd=' + formatDate(endDate);

  var headers = {
    'Authorization': 'Basic ' + Utilities.base64Encode(apiKey + ':' + apiSecret)
  };

  var options = {
    'headers': headers,
    'method': 'GET',
    'muteHttpExceptions': true
  };

  var responseData = [];
  var nextPage = 1;
  var totalPages = 1;

  // Fetch all pages of results
  while (nextPage <= totalPages) {
    var pageUrl = url + '&page=' + nextPage;

    var response = UrlFetchApp.fetch(pageUrl, options);
    var pageData = JSON.parse(response.getContentText());

    responseData = responseData.concat(pageData.orders);
    totalPages = pageData.pages;
    nextPage++;
  }

  var sheetName = 'Shipped';
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    sheet = spreadsheet.insertSheet(sheetName);
  }

  // Clear existing data in the sheet
  sheet.clear();

  // Write headers
  var orderHeaders = Object.keys(responseData[0]);
  var itemHeaders = Object.keys(responseData[0].items[0]);
  var headers = orderHeaders.concat(itemHeaders);
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  // Write data
  var data = [];

  responseData.forEach(function (order) {
    order.items.forEach(function (item) {
      var row = [];
      orderHeaders.forEach(function (header) {
        row.push(order[header]);
      });
      itemHeaders.forEach(function (header) {
        row.push(item[header]);
      });
      data.push(row);
    });
  });

  if (data.length > 0) {
    sheet.getRange(2, 1, data.length, data[0].length).setValues(data);
  }
}

// Helper function to format date in 'yyyy-MM-dd' format
function formatDate(date) {
  var year = date.getFullYear();
  var month = ('0' + (date.getMonth() + 1)).slice(-2);
  var day = ('0' + date.getDate()).slice(-2);
  return year + '-' + month + '-' + day;
}
