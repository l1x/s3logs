namespace S3logs


type FileState =
  | NotStarted
  | Downloaded
  | Unzipped
  | Merged
  | Uploaded
  | Done
  | FileStateError of err: string
  | ParsingError
