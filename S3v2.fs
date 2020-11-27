namespace S3logs

open Amazon
open Amazon.S3
open Amazon.S3.Model
open System.IO
open System
open System.Net
open System.Threading.Tasks
open Amazon.Runtime
open Newtonsoft.Json


module S3v2 =

  /// S3ReadError - common error handling for read operations (get, head, list)
  type S3ReadError =
    | NotFound of key: string
    | S3ReadPermissionDenied of keyOrPrefix: string
    | S3ReadException of keyOrPrefix: string * isRecoverable: bool * httpStatus: int option * ex: Exception option


  /// S3HeadReturn
  type S3HeadSuccess = S3HeadSuccess of key: string

  type S3HeadReturn = Result<S3HeadSuccess, S3ReadError>


  /// S3GetBytesReturn
  type S3GetBytesSuccess = S3GetBytesSuccess of key: string * value: byte []

  type S3GetBytesReturn = Result<S3GetBytesSuccess, S3ReadError>


  /// S3GetStreamReturn
  type S3GetStreamSuccess = S3GetStreamSuccess of key: string * value: Stream

  type S3GetStreamReturn = Result<S3GetStreamSuccess, S3ReadError>


  /// S3ListReturn
  type S3ListSuccess = S3ListSuccess of prefix: string * objects: List<S3Object>

  type S3ListReturn = Result<S3ListSuccess, S3ReadError>


  /// S3WriteError - common error handling for read operations (get, head, list)
  type S3WriteError =
    | S3WritePermissionDenied of keyOrPrefix: string
    | S3WriteException of keyOrPrefix: string * isRecoverable: bool * httpStatus: int option * ex: Exception option


  /// S3PutReturn
  type S3PutSuccess = S3PutSuccess of key: string

  type S3PutReturn = Result<S3PutSuccess, S3WriteError>


  /// S3DeleteReturn
  type S3DeleteSuccess = S3DeleteSuccess of key: string

  type S3DeleteReturn = Result<S3DeleteSuccess, S3WriteError>

  /// S3CopyReturn
  type S3CopySuccess = S3CopySuccess of bucket: string * key: string

  type S3CopyReturn = Result<S3CopySuccess, S3WriteError>

  /// S3MoveReturn
  type S3MoveSuccess = S3MoveSuccess of bucket: string * key: string

  type S3MoveReturn = Result<S3MoveSuccess, S3WriteError>

  type S3v2(awsS3Client: AmazonS3Client, log: string -> unit) =

    let readAllBytes (s: Stream) =
      let ms = new MemoryStream()
      s.CopyTo(ms)
      ms.ToArray()


    let rec handleReadException key (ex: Exception): S3ReadError =
      match ex with
      | :? AmazonS3Exception as s3e when s3e.StatusCode = HttpStatusCode.NotFound -> NotFound key
      | :? AmazonS3Exception as s3e when s3e.StatusCode = HttpStatusCode.Forbidden -> S3ReadPermissionDenied key
      | :? AmazonS3Exception as s3e -> S3ReadException(key, false, None, Some(s3e :> Exception))
      | :? AggregateException as ae ->
          ae.InnerExceptions
          |> Seq.map (handleReadException key)
          |> Seq.tryHead
          |> Option.defaultValue (S3ReadException(key, false, None, Some(ae :> Exception)))
      | _ -> S3ReadException(key, false, None, Some ex)


    let rec handleWriteException key (ex: Exception): S3WriteError =
      match ex with
      | :? AmazonS3Exception as s3e when s3e.StatusCode = HttpStatusCode.Forbidden -> S3WritePermissionDenied key
      | :? AmazonS3Exception as s3e -> S3WriteException(key, false, None, Some(s3e :> Exception))
      | :? AggregateException as ae ->
          ae.InnerExceptions
          |> Seq.map (handleWriteException key)
          |> Seq.tryHead
          |> Option.defaultValue (S3WriteException(key, false, None, Some(ae :> Exception)))
      | _ -> S3WriteException(key, false, None, Some ex)


    //
    // PUBLIC API
    //

    //
    //  READ - HEAD - ASYNC
    //

    member this.GetS3ObjectMetaAsync (bucket: string) (key: string): Async<S3HeadReturn> =
      async {
        try
          let! ct = Async.CancellationToken

          let request =
            GetObjectMetadataRequest(BucketName = bucket, Key = key)

          let task =
            awsS3Client.GetObjectMetadataAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK
          | HttpStatusCode.NoContent -> return Ok(S3HeadSuccess(key))
          | httpStatus -> return Error(S3ReadException(key, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleReadException key ex)
      }

    //
    //  READ - HEAD - SYNC
    //

    member this.GetS3ObjectMeta (bucket: string) (key: string): S3HeadReturn =
      this.GetS3ObjectMetaAsync bucket key
      |> Async.RunSynchronously


    //
    //  READ - GET - ASYNC - STREAM
    //

    member this.GetS3ObjectStreamAsync (bucket: string) (key: string): Async<S3GetStreamReturn> =
      async {
        try
          let! ct = Async.CancellationToken

          let request =
            GetObjectRequest(BucketName = bucket, Key = key)

          let task = awsS3Client.GetObjectAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK -> return Ok(S3GetStreamSuccess(key, result.ResponseStream))
          | httpStatus -> return Error(S3ReadException(key, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleReadException key ex)
      }


    //
    //  READ - GET - ASYNC
    //

    member this.GetS3ObjectBytesAsync (bucket: string) (key: string): Async<S3GetBytesReturn> =
      async {
        try
          let! ct = Async.CancellationToken

          let request =
            GetObjectRequest(BucketName = bucket, Key = key)

          let task = awsS3Client.GetObjectAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK -> return Ok(S3GetBytesSuccess(key, (readAllBytes result.ResponseStream)))
          | httpStatus -> return Error(S3ReadException(key, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleReadException key ex)
      }


    //
    //  READ - GET - SYNC
    //

    member this.GetS3ObjectBytes (bucket: string) (key: string): S3GetBytesReturn =
      this.GetS3ObjectBytesAsync bucket key
      |> Async.RunSynchronously


    //
    //  READ - LIST - ASYNC
    //

    member this.ListS3ObjectsAsync (bucket: string) (prefix: string): Async<S3ListReturn> =
      // (maxKeys: int option) (continuationToken: string option) - TODO PAGINATE
      async {
        try
          let! ct = Async.CancellationToken

          let request =
            ListObjectsV2Request(BucketName = bucket, Prefix = prefix)

          let task =
            awsS3Client.ListObjectsV2Async(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK -> return Ok(S3ListSuccess(prefix, List.ofSeq result.S3Objects))
          | httpStatus -> return Error(S3ReadException(prefix, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleReadException prefix ex)
      }

    //
    //  READ - LIST - SYNC
    //

    member this.ListS3Objects (bucket: string) (prefix: string): S3ListReturn =
      // (maxKeys: int option) (continuationToken: string option) - TODO PAGINATE
      this.ListS3ObjectsAsync bucket prefix
      |> Async.RunSynchronously


    //
    //  WRITE - PUT - ASYNC
    //

    member this.PutS3ObjectStreamAsync (bucket: string) (key: string) (contentType: string) (value: System.IO.Stream)
                                       : Async<S3PutReturn> =
      async {
        let! ct = Async.CancellationToken

        try
          let request =
            PutObjectRequest(BucketName = bucket, Key = key, InputStream = value, ContentType = contentType)

          let task = awsS3Client.PutObjectAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK
          | HttpStatusCode.NoContent -> return Ok(S3PutSuccess(key))
          | httpStatus -> return Error(S3WriteException(key, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleWriteException key ex)
      }


    member this.PutS3ObjectBytesAsync (bucket: string) (key: string) (contentType: string) (value: byte [])
                                      : Async<S3PutReturn> =
      async {
        let! ct = Async.CancellationToken

        use ms = new MemoryStream(value)

        return! this.PutS3ObjectStreamAsync bucket key contentType ms
      }

    member this.PutS3ObjectStringAsync (bucket: string) (key: string) (contentType: string) (value: string)
                                       : Async<S3PutReturn> =
      async {
        let! ct = Async.CancellationToken

        try
          let request =
            PutObjectRequest(BucketName = bucket, Key = key, ContentBody = value, ContentType = contentType)

          let task = awsS3Client.PutObjectAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK
          | HttpStatusCode.NoContent -> return Ok(S3PutSuccess(key))
          | httpStatus -> return Error(S3WriteException(key, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleWriteException key ex)
      }

    //
    //  WRITE - PUT - SYNC
    //

    member this.PutS3ObjectBytes (bucket: string) (key: string) (contentType: string) (value: byte []): S3PutReturn =
      this.PutS3ObjectBytesAsync bucket key contentType value
      |> Async.RunSynchronously


    member this.PutS3ObjectStream (bucket: string) (key: string) (contentType: string) (value: System.IO.Stream)
                                  : S3PutReturn =
      this.PutS3ObjectStreamAsync bucket key contentType value
      |> Async.RunSynchronously


    member this.PutS3ObjectString (bucket: string) (key: string) (contentType: string) (value: string): S3PutReturn =
      this.PutS3ObjectStringAsync bucket key contentType value
      |> Async.RunSynchronously

    //
    //  WRITE - DELETE - ASYNC
    //

    member this.DeleteS3ObjectAsync (bucket: string) (key: string): Async<S3DeleteReturn> =
      async {
        try
          let! ct = Async.CancellationToken

          let request =
            DeleteObjectRequest(BucketName = bucket, Key = key)

          let task =
            awsS3Client.DeleteObjectAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK
          | HttpStatusCode.NoContent -> return Ok(S3DeleteSuccess(key))
          | httpStatus -> return Error(S3WriteException(key, false, (Some(int httpStatus)), None))

        with ex -> return Error(handleWriteException key ex)
      }

    //
    //  WRITE - DELETE - SYNC
    //

    member this.DeleteS3Object (bucket: string) (key: string): S3DeleteReturn =
      // (maxKeys: int option) (continuationToken: string option) - TODO PAGINATE
      this.DeleteS3ObjectAsync bucket key
      |> Async.RunSynchronously


    //
    //  WRITE - COPY - ASYNC
    //

    member this.CopyObjectAsync (sourceBucket: string)
                                (sourceKey: string)
                                (destinationBucket: string)
                                (destinationKey: string)
                                : Async<S3CopyReturn> =
      async {
        try
          let! ct = Async.CancellationToken

          let request =
            CopyObjectRequest
              (SourceBucket = sourceBucket,
               SourceKey = sourceKey,
               DestinationBucket = destinationBucket,
               DestinationKey = destinationKey)

          let task = awsS3Client.CopyObjectAsync(request, ct)

          let! result = task |> Async.AwaitTask

          match result.HttpStatusCode with
          | HttpStatusCode.OK
          | HttpStatusCode.NoContent -> return Ok(S3CopySuccess(destinationBucket, destinationKey))
          | httpStatus -> return Error(S3WriteException(destinationKey, false, (Some(int httpStatus)), None))
        with ex -> return Error(handleWriteException destinationKey ex)
      }


    //
    //  WRITE - COPY - SYNC
    //
    member this.CopyObject (sourceBucket: string)
                           (sourceKey: string)
                           (destinationBucket: string)
                           (destinationKey: string)
                           : S3CopyReturn =
      this.CopyObjectAsync sourceBucket sourceKey destinationBucket destinationKey
      |> Async.RunSynchronously

    //
    //  WRITE - MOVE - ASYNC
    //

    member this.MoveObjectAsync (sourceBucket: string)
                                (sourceKey: string)
                                (destinationBucket: string)
                                (destinationKey: string)
                                : Async<S3MoveReturn> =
      async {
        try
          let! copied = this.CopyObjectAsync sourceBucket sourceKey destinationBucket destinationKey

          let! deleted =
            match copied with
            | Ok (S3CopySuccess (b, k)) -> this.DeleteS3ObjectAsync sourceBucket sourceKey
            | Error err -> async { return Error err }

          return match deleted with
                 | Ok (S3DeleteSuccess k) -> Ok(S3MoveSuccess(destinationBucket, destinationKey))
                 | Error err -> Error err
        with ex -> return Error(handleWriteException destinationKey ex)
      }


    //
    //  WRITE - MOVE - SYNC
    //
    member this.MoveObject (sourceBucket: string)
                           (sourceKey: string)
                           (destinationBucket: string)
                           (destinationKey: string)
                           : S3MoveReturn =
      this.MoveObjectAsync sourceBucket sourceKey destinationBucket destinationKey
      |> Async.RunSynchronously

  // Creating the instance

  let CreateS3 region log =
    let awsS3Client =
      log
      <| sprintf "Connecting to S3 :: region: %s" region
      let region = RegionEndpoint.GetBySystemName(region)

      let config =
        AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(20000.0)))

      let client = new AmazonS3Client(config)
      log
      <| sprintf "S3 client config :: %A" (JsonConvert.SerializeObject(client.Config))
      client

    S3v2(awsS3Client, log)
