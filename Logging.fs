namespace S3logs

// external
open System
open System.IO

// internal

module Logging =

    type LogLevel =
        | Info
        | Debug


    type Logger(name, logLevel, timestampFn: Unit -> DateTime) =

        let logLevelActual logLevel =
            match logLevel with
            | "info" -> Info
            | "debug" -> Debug
            | _ -> failwith "Not supported loglevel"

        let currentTime (tw: TextWriter) = tw.Write("{0:s}", timestampFn ())

        let logEvent level msg =
            printfn "%t %s [%s] %s" currentTime level name msg

        member this.LogInfo msg = logEvent "INFO" msg |> ignore

        member this.LogError msg = logEvent "ERROR" msg |> ignore

        member this.LogDebug msg =
            if logLevelActual logLevel = Debug then logEvent "DEBUG" msg

        static member CreateLogger name logLevel timestampFn = Logger(name, logLevel, timestampFn)
