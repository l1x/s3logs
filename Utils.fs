namespace S3logs

// internal

//external


module Utils =


  let tryParseWith (tryParseFunc: string -> bool * _) =
    tryParseFunc
    >> function
    | true, v -> Some v
    | false, _ -> None


  let parseDate = tryParseWith System.DateTime.TryParse
  let parseInt = tryParseWith System.Int32.TryParse
  let parseSingle = tryParseWith System.Single.TryParse
  let parseDouble = tryParseWith System.Double.TryParse
  let parseGuid = tryParseWith System.Guid.TryParse


  let tee (log: string -> unit) x =
    log <| sprintf "%A" x
    x
