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

    /// Common error cases for S3 read operations

    type S3ReadError =
        | NotFound of key: string
        | PermissionDenied of key: string
        | S3ReadError of key: string * isRecoverable: bool * httpStatus: int option * ex: Exception option

    /// S3 GetObject Success

    type S3ReadSuccess = S3ReadSuccess of key: string * value: byte [] option

    /// S3 GetObject Result -- GET

    type S3ReadReturn = Result<S3ReadSuccess, S3ReadError>


    type S3TaskType =
        | GetObject of Task<GetObjectResponse>
        | GetMetadata of Task<GetObjectMetadataResponse>

    type S3v2(awsS3Client: AmazonS3Client, log: string -> unit) =


        let readAllBytes (s: Stream) =
            let ms = new MemoryStream()
            s.CopyTo(ms)
            ms.ToArray()


        let rec handleReadException key (ex: Exception): S3ReadError =
            match ex with
            | :? AmazonS3Exception as s3e when s3e.StatusCode = HttpStatusCode.NotFound -> NotFound key
            | :? AmazonS3Exception as s3e when s3e.StatusCode = HttpStatusCode.Forbidden -> PermissionDenied key
            | :? AmazonS3Exception as s3e -> S3ReadError(key, false, None, Some(s3e :> Exception))
            | :? AggregateException as ae ->
                ae.InnerExceptions
                |> Seq.map (handleReadException key)
                |> Seq.tryHead
                |> Option.defaultValue (S3ReadError(key, false, None, Some(ae :> Exception)))
            | _ -> S3ReadError(key, false, None, Some ex)


        member private this.GetS3ObjectInternal (bucket: string) (key: string) (s3Task: S3TaskType): S3ReadReturn =
            try
                let waitAndUpcast (x: Task<'t>) =
                    let t =
                        x |> Async.AwaitTask |> Async.RunSynchronously

                    x.Result :> AmazonWebServiceResponse

                let result =
                    match s3Task with
                    | GetObject x -> waitAndUpcast x
                    | GetMetadata x -> waitAndUpcast x

                match result.HttpStatusCode with
                | HttpStatusCode.OK ->
                    match result with
                    | :? GetObjectResponse as res -> Ok(S3ReadSuccess(key, Some(readAllBytes (res.ResponseStream))))
                    | :? GetObjectMetadataResponse as _res -> Ok(S3ReadSuccess(key, None))
                    | _ -> Error(S3ReadError(key, false, Some(int HttpStatusCode.OK), None))
                | HttpStatusCode.NotFound -> Error(NotFound key)
                | httpStatus -> Error(S3ReadError(key, false, (Some(int httpStatus)), None))
            with ex -> Error(handleReadException key ex)

        member private this.GetS3ObjectInternalAsync<'T when 'T :> AmazonWebServiceResponse> (bucket: string)
                                                                                             (key: string)
                                                                                             (s3Task: Task<'T>)
                                                                                             : Async<S3ReadReturn> =
            async {
                try
                    let! result = s3Task |> Async.AwaitTask

                    match result.HttpStatusCode with
                    | HttpStatusCode.OK ->
                        match box result with
                        | :? GetObjectResponse as res ->
                            return Ok(S3ReadSuccess(key, Some(readAllBytes (res.ResponseStream))))
                        | :? GetObjectMetadataResponse as _res -> return Ok(S3ReadSuccess(key, None))
                        | _ -> return Error(S3ReadError(key, false, Some(int HttpStatusCode.OK), None))
                    | HttpStatusCode.NotFound -> return Error(NotFound key)
                    | httpStatus -> return Error(S3ReadError(key, false, (Some(int httpStatus)), None))
                with ex -> return Error(handleReadException key ex)
            }

        member this.GetS3ObjectBytesAsync(bucket: string, key: string): Async<S3ReadReturn> =
            async {
                let! ct = Async.CancellationToken

                let task =
                    let request =
                        GetObjectRequest(BucketName = bucket, Key = key)

                    awsS3Client.GetObjectAsync(request, ct)

                return! this.GetS3ObjectInternalAsync bucket key task
            }

        member this.GetS3ObjectMetaAsync(bucket: string, key: string): Async<S3ReadReturn> =
            async {
                let! ct = Async.CancellationToken

                let task =
                    let request =
                        GetObjectMetadataRequest(BucketName = bucket, Key = key)

                    awsS3Client.GetObjectMetadataAsync(request, ct)

                return! this.GetS3ObjectInternalAsync bucket key task
            }

        //
        //  READ - HEAD
        //

        member this.GetS3ObjectMeta (bucket: string) (key: string): S3ReadReturn =
            let task =
                awsS3Client.GetObjectMetadataAsync
                <| GetObjectMetadataRequest(BucketName = bucket, Key = key)

            this.GetS3ObjectInternal bucket key (GetMetadata task)


        //
        //  READ - GET
        //

        member this.GetS3ObjectBytes (bucket: string) (key: string): S3ReadReturn =
            let task =
                awsS3Client.GetObjectAsync
                <| GetObjectRequest(BucketName = bucket, Key = key)

            this.GetS3ObjectInternal bucket key (GetObject task)


    //
    //  WRITE - PUT
    //


    //
    //  DELETE - DELETE
    //


    //
    //  MOVE - COPY
    //



    let CreateS3 region log =
        let awsS3Client =
            log
            <| sprintf "Connecting to S3 :: region: %s" region
            let region = RegionEndpoint.GetBySystemName(region)

            let config =
                AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(500.0)))

            let client = new AmazonS3Client(config)
            log
            <| sprintf "S3 client config :: %A" (JsonConvert.SerializeObject(client.Config))
            client

        S3v2(awsS3Client, log)
