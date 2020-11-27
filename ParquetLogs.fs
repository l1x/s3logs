namespace S3logs

// Internal

// External
open Parquet
open Parquet.Data
open System.Collections.Generic
open System.IO
open System
open Logging

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

module ParquetLogs =


  let loggerParquetLogs =
    Logger.CreateLogger "ParquetLogs" "info" (fun _ -> DateTime.Now)

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
      CsBytesColumn: List<int>
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
      ScContentLenColumn: List<int>
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
      ScBytes = DataField("ScBytes", DataType.Int32)
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
      CsBytes = DataField("CsBytes", DataType.Int32)
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
      ScContentLen = DataField("ScContentLen", DataType.Int32)
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
      CsBytesColumn = new List<int>()
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
      ScContentLenColumn = new List<int>()
      ScRangeStartColumn = new List<string>()
      ScRangeEndColumn = new List<string>() }


  let createNewParquetWriter (stream: MemoryStream) =
    let parquetWriter = new ParquetWriter(parquetSchema, stream)
    parquetWriter.CompressionMethod <- CompressionMethod.Gzip
    parquetWriter


  let fillParquetColumns (parquetColumns: ParquetColumns) =

    use stream = new MemoryStream()

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
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.XForwardedFor, parquetColumns.XForwardedForColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.SslProtocol, parquetColumns.SslProtocolColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.SslCipher, parquetColumns.SslCipherColumn.ToArray()))
    rowGroup.WriteColumn
      (DataColumn(parquetFieldMap.XEdgeResponseResultType, parquetColumns.XEdgeResponseResultTypeColumn.ToArray()))
    rowGroup.WriteColumn
      (DataColumn(parquetFieldMap.CsProtocolVersion, parquetColumns.CsProtocolVersionColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.FleStatus, parquetColumns.FleStatusColumn.ToArray()))
    rowGroup.WriteColumn
      (DataColumn(parquetFieldMap.FleEncryptedFields, parquetColumns.FleEncryptedFieldsColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.CPort, parquetColumns.CPortColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.TimeToFirstByte, parquetColumns.TimeToFirstByteColumn.ToArray()))
    rowGroup.WriteColumn
      (DataColumn(parquetFieldMap.XEdgeDetailedResultType, parquetColumns.XEdgeDetailedResultTypeColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.ScContentType, parquetColumns.ScContentTypeColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.ScContentLen, parquetColumns.ScContentLenColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.ScRangeStart, parquetColumns.ScRangeStartColumn.ToArray()))
    rowGroup.WriteColumn(DataColumn(parquetFieldMap.ScRangeEnd, parquetColumns.ScRangeEndColumn.ToArray()))
    rowGroup.Dispose()
    parquetWriter.Dispose()
    stream.ToArray()


  let processLogRows (rows: seq<string array>): byte [] =

    let parquetColumns = createParquetColumns ()

    for row in rows do
      parquetColumns.DateTimeColumn.Add(row.[0])
      parquetColumns.EdgeLocationColumn.Add(row.[1])
      parquetColumns.ScBytesColumn.Add(Utils.parseInt (row.[2]) |> Option.defaultValue 0)
      parquetColumns.CIpColumn.Add(row.[3])
      parquetColumns.CsMethodColumn.Add(row.[4])
      parquetColumns.CsHostColumn.Add(row.[5])
      parquetColumns.CsUriStemColumn.Add(row.[6])
      parquetColumns.ScStatusColumn.Add(row.[7])
      parquetColumns.CsRefererColumn.Add(row.[8])
      parquetColumns.CsUserAgentColumn.Add(row.[9])
      parquetColumns.CsUriQueryColumn.Add(row.[10])
      parquetColumns.CsCookieColumn.Add(row.[11])
      parquetColumns.XEdgeResultTypeColumn.Add(row.[12])
      parquetColumns.XEdgeRequestIdColumn.Add(row.[13])
      parquetColumns.XHostHeaderColumn.Add(row.[14])
      parquetColumns.CsProtocolColumn.Add(row.[15])
      parquetColumns.CsBytesColumn.Add(Utils.parseInt (row.[16]) |> Option.defaultValue 0)
      parquetColumns.TimeTakenColumn.Add(row.[17])
      parquetColumns.XForwardedForColumn.Add(row.[18])
      parquetColumns.SslProtocolColumn.Add(row.[19])
      parquetColumns.SslCipherColumn.Add(row.[20])
      parquetColumns.XEdgeResponseResultTypeColumn.Add(row.[21])
      parquetColumns.CsProtocolVersionColumn.Add(row.[22])
      parquetColumns.FleStatusColumn.Add(row.[23])
      parquetColumns.FleEncryptedFieldsColumn.Add(row.[24])
      parquetColumns.CPortColumn.Add(row.[25])
      parquetColumns.TimeToFirstByteColumn.Add(row.[26])
      parquetColumns.XEdgeDetailedResultTypeColumn.Add(row.[27])
      parquetColumns.ScContentTypeColumn.Add(row.[28])
      parquetColumns.ScContentLenColumn.Add(Utils.parseInt (row.[29]) |> Option.defaultValue 0)
      parquetColumns.ScRangeStartColumn.Add(row.[30])
      parquetColumns.ScRangeEndColumn.Add(row.[31])

    fillParquetColumns parquetColumns
