module.exports = function (doc) {
  if (!doc.userId) {
    return false;
  }

  var year = doc.createdAt.Year();
  var month = doc.createdAt.Month();
  if (month < 10) {
    month = "0" + month;
  }

  delete doc.__v
  delete doc.type
  delete doc.name
  delete doc.status

  // Set index name
  var meta = { type: doc.type, index: "es-index-" + year + month };
  doc._meta_monstache = meta;
  return doc;
};
