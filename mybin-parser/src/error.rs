use nom::error::{ParseError, VerboseError, VerboseErrorKind};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid code")]
    InvalidColumnTypeCode(u32),
    #[error("inconsistent checksum")]
    InconsistentChecksum(u32, u32),
    #[error("incomplete input: {0:?}")]
    Incomplete(nom::Needed),
    #[error("parse error: {0}")]
    ParseErr(String),
    #[error("utf8 error")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

type InputAndNomError<'a> = (&'a [u8], nom::Err<VerboseError<&'a [u8]>>);

impl<'a> From<InputAndNomError<'a>> for Error {
    fn from((input, err): InputAndNomError<'a>) -> Error {
        match err {
            nom::Err::Error(e) | nom::Err::Failure(e) => Error::ParseErr(fmt_nom_err(input, e)),
            nom::Err::Incomplete(n) => Error::ParseErr(format!("more input required: {:?}", n)),
        }
    }
}

// impl From<std::string::FromUtf8Error> for Error {
//     fn from(err: std::string::FromUtf8Error) -> Error {
//         Error::FromUtf8Error(err)
//     }
// }

/// convert VerboseError to displayable format
/// reference: https://docs.rs/nom/5.1.2/src/nom/error.rs.html#136-229
pub fn fmt_nom_err<'a>(input: &'a [u8], e: VerboseError<&'a [u8]>) -> String {
    use nom::Offset;
    use std::fmt::Write;
    let mut result = String::new();
    for (i, (subinput, kind)) in e.errors.iter().enumerate() {
        let offset = input.offset(subinput);
        if input.is_empty() {
            match kind {
                VerboseErrorKind::Char(c) => {
                    write!(&mut result, "{}: expected '{}', got empty input\n\n", i, c)
                }
                VerboseErrorKind::Context(s) => {
                    write!(&mut result, "{}: in {}, got empty input\n\n", i, s)
                }
                VerboseErrorKind::Nom(e) => {
                    write!(&mut result, "{}: in {:?}, got empty input\n\n", i, e)
                }
            }
            .unwrap();
        } else {
            let prefix = &input[..offset];
            fmt_input(&mut result, prefix);
            match kind {
                VerboseErrorKind::Char(c) => {
                    if let Some(actual) = subinput.get(0) {
                        write!(&mut result, "expected '{}', found '{}'\n", c, actual).unwrap();
                    } else {
                        write!(&mut result, "expected '{}', got end of input\n", c).unwrap();
                    }
                }
                VerboseErrorKind::Context(s) => {
                    write!(&mut result, "in {}\n", s).unwrap();
                }
                VerboseErrorKind::Nom(e) => {
                    write!(&mut result, "in {:?}\n", e).unwrap();
                }
            }
        }
    }
    result
}

fn fmt_input(out: &mut String, input: &[u8]) {
    use std::fmt::Write;
    const PER_LINE: usize = 48;
    let mut last_line = 0;

    for i in input {
        if last_line >= PER_LINE {
            out.push('\n');
            last_line = 1;
        }
        write!(out, "{:02x} ", i).unwrap();
        last_line += 3;
    }
    out.push('\n');
    println!("{}", last_line);
    write!(
        out,
        "{caret:>columns$}\n",
        caret = '^',
        columns = last_line - 3
    )
    .unwrap();
}
