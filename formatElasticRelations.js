module.exports = function (doc) {
  if (
    !doc.clientSettings ||
    !doc.clientSettings.supplierClientId ||
    !doc.clientSettings.customerClientId
  ) {
    var clientSettings = pipe(
      [
        {
          $project: {
            supplierClientId: {
              $cond: [
                {
                  $in: [
                    doc.supplier.companyId,
                    { $setUnion: ["$subsidiaryIds", ["$companyId"]] },
                  ],
                },
                "$clientId",
                "$$REMOVE",
              ],
            },
            customerClientId: {
              $cond: [
                {
                  $in: [
                    doc.customer.companyId,
                    { $setUnion: ["$subsidiaryIds", ["$companyId"]] },
                  ],
                },
                "$clientId",
                "$$REMOVE",
              ],
            },
          },
        },
        {
          $group: {
            _id: null,
            customerClientIds: {
              $addToSet: "$customerClientId",
            },
            supplierClientIds: {
              $addToSet: "$supplierClientId",
            },
          },
        },
        {
          $project: {
            _id: 0,
            supplierClientId: { $first: "$supplierClientIds" },
            customerClientId: { $first: "$customerClientIds" },
          },
        },
      ],
      { collection: "clientSettings" }
    )[0];

    if (clientSettings) {
      doc.supplierClientId = clientSettings.supplierClientId;
      doc.customerClientId = clientSettings.customerClientId;
    }
  }

  if (doc.supplier || doc.customer) {
    if (doc.supplier && doc.supplier.companyName) {
      doc.supplierName = doc.supplier.companyName.toLowerCase();
    }
    if (doc.customer && doc.customer.companyName) {
      doc.customerName = doc.customer.companyName.toLowerCase();
    }

    var addressFields = ["street", "zipCode", "city", "country", "countryCode"];
    for (var i = 0; i < addressFields.length; i++) {
      if (
        doc.supplier &&
        doc.supplier.address &&
        doc.supplier.address[addressFields[i]]
      ) {
        doc["supplierAddress_" + addressFields[i]] =
          doc.supplier.address[addressFields[i]].toLowerCase();
      }
      if (
        doc.customer &&
        doc.customer.address &&
        doc.customer.address[addressFields[i]]
      ) {
        doc["customerAddress_" + addressFields[i]] =
          doc.customer.address[addressFields[i]].toLowerCase();
      }
    }
    delete doc.supplier;
    delete doc.customer;
  }

  return doc;
};
