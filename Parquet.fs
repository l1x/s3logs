namespace S3logs

// Internal

// External
open Parquet
open Parquet.Data
open System.Collections.Generic
open System.IO
open System

// "2020-06-02T09:37:21"; "DUS51-C1";     "780";    "1.2.3.4";  "GET";
// dateTime               x-edge-location sc-bytes  c-ip        cs-method

// "dbrgct5gwrbsd.cloudfront.net";  "/index.html";    "502";      "-";
// cs(Host)                         cs-uri-stem       sc-status   cs(Referer)

// cs(User-Agent)
// "Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_14_6)%20AppleWebKit/605.1.15%20(KHTML,%20like%20Gecko)%20Version/13.1.1%20Safari/605.1.15";

// cs-uri-query cs(Cookie)  x-edge-result-type  x-edge-request-id
// "-";         "-";        "Error";            "XzZdU1Qa581mxpRWzwvF1NAT4I43kr7-bGM6FQAw6l-XMTQw5nhyow==";

// x-host-header                    cs-protocol cs-bytes  time-taken    x-forwarded-for ssl-protocol
// "dbrgct5gwrbsd.cloudfront.net"; "https";     "17";     "0.869";      "-";            "TLSv1.2";

// ssl-cipher                       x-edge-response-result-type cs-protocol-version  fle-status fle-encrypted-fields  c-port
// "ECDHE-RSA-AES128-GCM-SHA256";   "Error";                    "HTTP/2.0";          "-";       "-";                  "51045";

// time-to-first-byte   x-edge-detailed-result-type     sc-content-type   sc-content-len  sc-range-start  sc-range-end
// "0.869";             "OriginDnsError";               "text/html";      "507";          "-";            "-"

module ParquetCsv =

  let egy = 1

// type ParquetColumns =
//   { DateTimes: List<string>
//     EdgeLocations: List<string>
//     ScBytess: List<int>
//     CIps: List<string>
//     CsMethods: List<string>
//     CsHosts: List<string>
//     CsUriStems: List<string>
//     ScStatuses: List<string>
//     CsReferers: List<string>
//     CsUserAgents: List<string>
//     CsUriQueries: List<string>
//     CsCookies: List<string>
//     XEdgeResultTypes: List<string>
//     XEdgeRequestIds: List<string>
//     XHostHeaders: List<string>
//     CsProtocols: List<string>
//     CsBytes: List<string>
//     TimeTaken: List<string>
//     XForwardedFor: List<string>
//     SslProtocol: List<string>
//     SslCipher: List<string>
//     XEdgeResponseResultType: List<string>
//     CsProtocolVersion: List<string>
//     FleStatus: List<string>
//     FleEncryptedFields: List<string>
//     CPort: List<string>
//     TimeToFirstByte: List<string>
//     XEdgeDetailedResultType: List<string>
//     ScContentType: List<string>
//     ScContentLen: List<string>
//     ScRangeStart: List<string>
//     ScRangeEnd: List<string> }



// type ParquetFieldMap =
//   { DateTime: DataField
//     EdgeLocation: DataField
//     ScBytes: DataField
//     CIp: DataField
//     CsMethod: DataField
//     CsHost: DataField
//     CsUriStem: DataField
//     ScStatus: DataField
//     CsReferer: DataField
//     CsUserAgent: DataField
//     CsUriQuery: DataField
//     CsCookie: DataField
//     XEdgeResultType: DataField
//     XEdgeRequestId: DataField
//     XHostHeader: DataField
//     CsProtocol: DataField
//     CsBytes: DataField
//     TimeTaken: DataField
//     XForwardedFor: DataField
//     SslProtocol: DataField
//     SslCipher: DataField
//     XEdgeResponseResultType: DataField
//     CsProtocolVersion: DataField
//     FleStatus: DataField
//     FleEncryptedFields: DataField
//     CPort: DataField
//     TimeToFirstByte: DataField
//     XEdgeDetailedResultType: DataField
//     ScContentType: DataField
//     ScContentLen: DataField
//     ScRangeStart: DataField
//     ScRangeEnd: DataField }


// let parquetFieldMap: ParquetFieldMap =
//   { DateTime = DataField("DateTime", DataType.String)
//     EdgeLocation = DataField("EdgeLocation", DataType.String)
//     ScBytes = DataField("ScBytes", DataType.String)
//     CIp = DataField("CIp", DataType.String)
//     CsMethod = DataField("CsMethod", DataType.String)
//     CsHost = DataField("CsHost", DataType.String)
//     CsUriStem = DataField("CsUriStem", DataType.String)
//     ScStatus = DataField("ScStatus", DataType.String)
//     CsReferer = DataField("CsReferer", DataType.String)
//     CsUserAgent = DataField("CsUserAgent", DataType.String)
//     CsUriQuery = DataField("CsUriQuery", DataType.String)
//     CsCookie = DataField("CsCookie", DataType.String)
//     XEdgeResultType = DataField("XEdgeResultType", DataType.String)
//     XEdgeRequestId = DataField("XEdgeRequestId", DataType.String)
//     XHostHeader = DataField("XHostHeader", DataType.String)
//     CsProtocol = DataField("CsProtocol", DataType.String)
//     CsBytes = DataField("CsBytes", DataType.String)
//     TimeTaken = DataField("TimeTaken", DataType.String)
//     XForwardedFor = DataField("XForwardedFor", DataType.String)
//     SslProtocol = DataField("SslProtocol", DataType.String)
//     SslCipher = DataField("SslCipher", DataType.String)
//     XEdgeResponseResultType = DataField("XEdgeResponseResultType", DataType.String)
//     CsProtocolVersion = DataField("CsProtocolVersion", DataType.String)
//     FleStatus = DataField("FleStatus", DataType.String)
//     FleEncryptedFields = DataField("FleEncryptedFields", DataType.String)
//     CPort = DataField("CPort", DataType.String)
//     TimeToFirstByte = DataField("TimeToFirstByte", DataType.String)
//     XEdgeDetailedResultType = DataField("XEdgeDetailedResultType", DataType.String)
//     ScContentType = DataField("ScContentType", DataType.String)
//     ScContentLen = DataField("ScContentLen", DataType.String)
//     ScRangeStart = DataField("ScRangeStart", DataType.String)
//     ScRangeEnd = DataField("ScRangeEnd", DataType.String) }


// let parquetSchema =
//   Schema
//     (parquetFieldMap.DateTime,
//      parquetFieldMap.EdgeLocation,
//      parquetFieldMap.ScBytes,
//      parquetFieldMap.CIp,
//      parquetFieldMap.CsMethod,
//      parquetFieldMap.CsHost,
//      parquetFieldMap.CsUriStem,
//      parquetFieldMap.ScStatus,
//      parquetFieldMap.CsReferer,
//      parquetFieldMap.CsUserAgent,
//      parquetFieldMap.CsUriQuery,
//      parquetFieldMap.CsCookie,
//      parquetFieldMap.XEdgeResultType,
//      parquetFieldMap.XEdgeRequestId,
//      parquetFieldMap.XHostHeader,
//      parquetFieldMap.CsProtocol,
//      parquetFieldMap.CsBytes,
//      parquetFieldMap.TimeTaken,
//      parquetFieldMap.XForwardedFor,
//      parquetFieldMap.SslProtocol,
//      parquetFieldMap.SslCipher,
//      parquetFieldMap.XEdgeResponseResultType,
//      parquetFieldMap.CsProtocolVersion,
//      parquetFieldMap.FleStatus,
//      parquetFieldMap.FleEncryptedFields,
//      parquetFieldMap.CPort,
//      parquetFieldMap.TimeToFirstByte,
//      parquetFieldMap.XEdgeDetailedResultType,
//      parquetFieldMap.ScContentType,
//      parquetFieldMap.ScContentLen,
//      parquetFieldMap.ScRangeStart,
//      parquetFieldMap.ScRangeEnd)

// let createParquetColumns () = { DateTimes = new List<string>() }


// let createNewParquetWriter (stream: MemoryStream) =
//   let parquetWriter = new ParquetWriter(parquetSchema, stream)
//   parquetWriter.CompressionMethod <- CompressionMethod.Gzip
//   parquetWriter


// let fillParquetColumns (parquetColumns: ParquetColumns) =
//   let mutable arr: byte [] = null
//   (let stream = new MemoryStream()
//    let parquetWriter = createNewParquetWriter (stream)
//    let rowGroup = parquetWriter.CreateRowGroup()
//    rowGroup.WriteColumn(DataColumn(parquetFieldMap.UtilityProviderId, parquetColumns.UtilityProviderIds.ToArray()))
//    rowGroup.WriteColumn(DataColumn(parquetFieldMap.PodName, parquetColumns.PodNames.ToArray()))
//    rowGroup.WriteColumn(DataColumn(parquetFieldMap.VariableName, parquetColumns.VariableNames.ToArray()))
//    rowGroup.WriteColumn(DataColumn(parquetFieldMap.Timestamp, parquetColumns.Timestamps.ToArray()))
//    rowGroup.WriteColumn(DataColumn(parquetFieldMap.Value, parquetColumns.Values.ToArray()))
//    rowGroup.WriteColumn(DataColumn(parquetFieldMap.State, parquetColumns.States.ToArray()))
//    rowGroup.Dispose()
//    parquetWriter.Dispose()
//    arr <- stream.ToArray()
//    stream.Dispose())
//   arr

// let csvToParquetBytes (utilityProviderUuid) (rows: seq<CsvCommon.Row>) =

//   let parquetColumns = createParquetColumns ()

//   let utilityProviderIdString = sprintf "%A" utilityProviderUuid

//   for row in rows do
//     parquetColumns.UtilityProviderIds.Add(utilityProviderIdString)
//     parquetColumns.PodNames.Add(row.PodName)
//     parquetColumns.VariableNames.Add(row.VariableName)
//     parquetColumns.Timestamps.Add(row.Timestamp)
//     parquetColumns.Values.Add(row.Value)
//     parquetColumns.States.Add(row.State)

//   fillParquetColumns parquetColumns
