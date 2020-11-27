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

  type ParquetColumns =
    { DateTimeColumn: List<string>
      EdgeLocationColumn: List<string>
      ScBytesColumn: List<int>
      CIpColumn: List<string>
      CsMethodColumn: List<string>
      CsHostColumn: List<string>
      CsUriStemColumn: List<string>
      ScStatusColumn: List<string>
      CsRefererColumn: List<string>
      CsUserAgentColumn: List<string>
      CsUriQueryColumn: List<string>
      CsCookieColumn: List<string>
      XEdgeResultTypeColumn: List<string>
      XEdgeRequestIdColumn: List<string>
      XHostHeaderColumn: List<string>
      CsProtocolColumn: List<string>
      CsByteColumn: List<string>
      TimeTakenColumn: List<string>
      XForwardedForColumn: List<string>
      SslProtocolColumn: List<string>
      SslCipherColumn: List<string>
      XEdgeResponseResultTypeColumn: List<string>
      CsProtocolVersionColumn: List<string>
      FleStatusColumn: List<string>
      FleEncryptedFieldsColumn: List<string>
      CPortColumn: List<string>
      TimeToFirstByteColumn: List<string>
      XEdgeDetailedResultTypeColumn: List<string>
      ScContentTypeColumn: List<string>
      ScContentLenColumn: List<string>
      ScRangeStartColumn: List<string>
      ScRangeEndColumn: List<string> }


  type ParquetFieldMap =
    { DateTime: DataField
      EdgeLocation: DataField
      ScBytes: DataField
      CIp: DataField
      CsMethod: DataField
      CsHost: DataField
      CsUriStem: DataField
      ScStatus: DataField
      CsReferer: DataField
      CsUserAgent: DataField
      CsUriQuery: DataField
      CsCookie: DataField
      XEdgeResultType: DataField
      XEdgeRequestId: DataField
      XHostHeader: DataField
      CsProtocol: DataField
      CsBytes: DataField
      TimeTaken: DataField
      XForwardedFor: DataField
      SslProtocol: DataField
      SslCipher: DataField
      XEdgeResponseResultType: DataField
      CsProtocolVersion: DataField
      FleStatus: DataField
      FleEncryptedFields: DataField
      CPort: DataField
      TimeToFirstByte: DataField
      XEdgeDetailedResultType: DataField
      ScContentType: DataField
      ScContentLen: DataField
      ScRangeStart: DataField
      ScRangeEnd: DataField }


  let parquetFieldMap: ParquetFieldMap =
    { DateTime = DataField("DateTime", DataType.String)
      EdgeLocation = DataField("EdgeLocation", DataType.String)
      ScBytes = DataField("ScBytes", DataType.String)
      CIp = DataField("CIp", DataType.String)
      CsMethod = DataField("CsMethod", DataType.String)
      CsHost = DataField("CsHost", DataType.String)

      CsUriStem = DataField("CsUriStem", DataType.String)
      ScStatus = DataField("ScStatus", DataType.String)
      CsReferer = DataField("CsReferer", DataType.String)
      CsUserAgent = DataField("CsUserAgent", DataType.String)
      CsUriQuery = DataField("CsUriQuery", DataType.String)
      CsCookie = DataField("CsCookie", DataType.String)

      XEdgeResultType = DataField("XEdgeResultType", DataType.String)
      XEdgeRequestId = DataField("XEdgeRequestId", DataType.String)
      XHostHeader = DataField("XHostHeader", DataType.String)
      CsProtocol = DataField("CsProtocol", DataType.String)
      CsBytes = DataField("CsBytes", DataType.String)
      TimeTaken = DataField("TimeTaken", DataType.String)

      XForwardedFor = DataField("XForwardedFor", DataType.String)
      SslProtocol = DataField("SslProtocol", DataType.String)
      SslCipher = DataField("SslCipher", DataType.String)
      XEdgeResponseResultType = DataField("XEdgeResponseResultType", DataType.String)
      CsProtocolVersion = DataField("CsProtocolVersion", DataType.String)
      FleStatus = DataField("FleStatus", DataType.String)
      FleEncryptedFields = DataField("FleEncryptedFields", DataType.String)
      CPort = DataField("CPort", DataType.String)
      TimeToFirstByte = DataField("TimeToFirstByte", DataType.String)
      XEdgeDetailedResultType = DataField("XEdgeDetailedResultType", DataType.String)
      ScContentType = DataField("ScContentType", DataType.String)
      ScContentLen = DataField("ScContentLen", DataType.String)
      ScRangeStart = DataField("ScRangeStart", DataType.String)
      ScRangeEnd = DataField("ScRangeEnd", DataType.String) }


  let parquetSchema =
    Schema
      (parquetFieldMap.DateTime,
       parquetFieldMap.EdgeLocation,
       parquetFieldMap.ScBytes,
       parquetFieldMap.CIp,
       parquetFieldMap.CsMethod,
       parquetFieldMap.CsHost,
       parquetFieldMap.CsUriStem,
       parquetFieldMap.ScStatus,
       parquetFieldMap.CsReferer,
       parquetFieldMap.CsUserAgent,
       parquetFieldMap.CsUriQuery,
       parquetFieldMap.CsCookie,
       parquetFieldMap.XEdgeResultType,
       parquetFieldMap.XEdgeRequestId,
       parquetFieldMap.XHostHeader,
       parquetFieldMap.CsProtocol,
       parquetFieldMap.CsBytes,
       parquetFieldMap.TimeTaken,
       parquetFieldMap.XForwardedFor,
       parquetFieldMap.SslProtocol,
       parquetFieldMap.SslCipher,
       parquetFieldMap.XEdgeResponseResultType,
       parquetFieldMap.CsProtocolVersion,
       parquetFieldMap.FleStatus,
       parquetFieldMap.FleEncryptedFields,
       parquetFieldMap.CPort,
       parquetFieldMap.TimeToFirstByte,
       parquetFieldMap.XEdgeDetailedResultType,
       parquetFieldMap.ScContentType,
       parquetFieldMap.ScContentLen,
       parquetFieldMap.ScRangeStart,
       parquetFieldMap.ScRangeEnd)

  let createParquetColumns () =
    { DateTimeColumn = new List<string>()
      EdgeLocationColumn = new List<string>()
      ScBytesColumn = new List<int>()
      CIpColumn = new List<string>()
      CsMethodColumn = new List<string>()
      CsHostColumn = new List<string>()
      CsUriStemColumn = new List<string>()
      ScStatusColumn = new List<string>()
      CsRefererColumn = new List<string>()
      CsUserAgentColumn = new List<string>()
      CsUriQueryColumn = new List<string>()
      CsCookieColumn = new List<string>()
      XEdgeResultTypeColumn = new List<string>()
      XEdgeRequestIdColumn = new List<string>()
      XHostHeaderColumn = new List<string>()
      CsProtocolColumn = new List<string>()
      CsBytesColumn = new List<string>()
      TimeTakenColumn = new List<string>()
      XForwardedForColumn = new List<string>()
      SslProtocolColumn = new List<string>()
      SslCipherColumn = new List<string>()
      XEdgeResponseResultTypeColumn = new List<string>()
      CsProtocolVersionColumn = new List<string>()
      FleStatusColumn = new List<string>()
      FleEncryptedFieldsColumn = new List<string>()
      CPortColumn = new List<string>()
      TimeToFirstByteColumn = new List<string>()
      XEdgeDetailedResultTypeColumn = new List<string>()
      ScContentTypeColumn = new List<string>()
      ScContentLenColumn = new List<string>()
      ScRangeStartColumn = new List<string>()
      ScRangeEndColumn = new List<string>() }


  let createNewParquetWriter (stream: MemoryStream) =
    let parquetWriter = new ParquetWriter(parquetSchema, stream)
    parquetWriter.CompressionMethod <- CompressionMethod.Gzip
    parquetWriter


  let fillParquetColumns (parquetColumns: ParquetColumns) =
    let mutable arr: byte [] = null
    (let stream = new MemoryStream()
     let parquetWriter = createNewParquetWriter (stream)
     let rowGroup = parquetWriter.CreateRowGroup()

     rowGroup.WriteColumn(DataColumn(parquetFieldMap.DateTime, parquetColumns.DateTimeColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.EdgeLocation, parquetColumns.EdgeLocationColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.ScBytes, parquetColumns.ScBytesColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CIp, parquetColumns.CIpColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsMethod, parquetColumns.CsMethodColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsHost, parquetColumns.CsHostColumn.ToArray()))

     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsUriStem, parquetColumns.CsUriStemColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.ScStatus, parquetColumns.ScStatusColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsReferer, parquetColumns.CsRefererColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsUserAgent, parquetColumns.CsUserAgentColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsUriQuery, parquetColumns.CsUriQueryColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsCookie, parquetColumns.CsCookieColumn.ToArray()))

     rowGroup.WriteColumn(DataColumn(parquetFieldMap.XEdgeResultType, parquetColumns.XEdgeResultTypeColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.XEdgeRequestId, parquetColumns.XEdgeRequestIdColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.XHostHeader, parquetColumns.XHostHeaderColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsProtocol, parquetColumns.CsProtocolColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.CsBytes, parquetColumns.CsBytesColumn.ToArray()))
     rowGroup.WriteColumn(DataColumn(parquetFieldMap.TimeTaken, parquetColumns.CsCookieColumn.ToArray()))





     rowGroup.Dispose()
     parquetWriter.Dispose()
     arr <- stream.ToArray()
     stream.Dispose())
    arr

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
