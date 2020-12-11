namespace S3logs


open BenchmarkDotNet.Attributes


module Bm =

  module List =
    let inline myLength xs =
      let rec loop xs acc =
        match xs with
        | [] -> acc
        | _ :: tail -> loop tail (acc + 1)

      loop xs 0

  [<MemoryDiagnoser>]
  type LengthBench() =
    let lst = [ 1 .. 1_000_000 ]

    [<Benchmark(Baseline = true)>]
    member _.BuiltIn() = List.length lst

    [<Benchmark>]
    member _.Custom() = List.myLength lst
