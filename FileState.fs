namespace S3logs


type FileState =
  | NotStarted
  | Downloaded
  | Unzipped
  | Merged
  | Converted
  | FileStateError of err: string
  | ParsingError


// type ParquetFileState =
//   | Uploaded
//   | AvailableInAthena
