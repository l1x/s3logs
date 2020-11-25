namespace S3logs

// internal
open Cli
open Logging

//external
open System

module Main =

    let loggerMain =
        Logger.CreateLogger "Main" "info" (fun _ -> DateTime.Now)


    [<EntryPoint>]
    let main argv =
        let commandLineArgumentsParsed = parseCommandLine (Array.toList argv)
        loggerMain.LogInfo
        <| sprintf "%A" commandLineArgumentsParsed

        0
