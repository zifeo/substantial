//! Inputs as canonical JSON (RFC 8785). serde_jcs, as serde_json, writes a NaN or an infinity
//! inside a value as `null`, and rounds an integer past 2^53 - 1 to the double JCS numbers are:
//! both are refused here first.

use std::fmt;

use serde::Serialize;
use serde::ser;

use crate::error::{Error, Result};

/// The canonical JSON of an input: an object, or `{}` for `()` and `None`.
pub(crate) fn input_object(input: impl Serialize) -> Result<Vec<u8>> {
    let not_json =
        |e: &dyn fmt::Display| Error::InvalidArgument(format!("the input is not JSON: {e}"));
    input.serialize(Check).map_err(|e| not_json(&e))?;
    let bytes = serde_jcs::to_vec(&input).map_err(|e| not_json(&e))?;
    match bytes.as_slice() {
        b"null" => Ok(b"{}".to_vec()),
        [b'{', ..] => Ok(bytes),
        other => Err(Error::InvalidArgument(format!(
            "an input is a JSON object, not {}",
            String::from_utf8_lossy(other)
        ))),
    }
}

#[derive(Debug)]
struct Refused(String);

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

impl ser::Error for Refused {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

type Checked = std::result::Result<(), Refused>;

const SAFE: u128 = (1 << 53) - 1;

fn integer(magnitude: u128, shown: impl fmt::Display) -> Checked {
    if magnitude > SAFE {
        return Err(Refused(format!(
            "{shown} is past 2^53 - 1, the largest integer a JSON number holds exactly"
        )));
    }
    Ok(())
}

fn float(value: f64) -> Checked {
    if !value.is_finite() {
        return Err(Refused(format!("{value} is not a JSON number")));
    }
    Ok(())
}

/// Visits every number of a value, writing nothing.
struct Check;

macro_rules! fine {
    ($($method:ident($type:ty)),*) => {
        $(fn $method(self, _: $type) -> Checked { Ok(()) })*
    };
}

macro_rules! signed {
    ($($method:ident($type:ty)),*) => {
        $(fn $method(self, v: $type) -> Checked { integer(i128::from(v).unsigned_abs(), v) })*
    };
}

macro_rules! unsigned {
    ($($method:ident($type:ty)),*) => {
        $(fn $method(self, v: $type) -> Checked { integer(u128::from(v), v) })*
    };
}

impl ser::Serializer for Check {
    type Ok = ();
    type Error = Refused;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fine!(
        serialize_bool(bool),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8])
    );
    signed!(
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128)
    );
    unsigned!(
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128)
    );

    fn serialize_f32(self, v: f32) -> Checked {
        float(f64::from(v))
    }
    fn serialize_f64(self, v: f64) -> Checked {
        float(v)
    }
    fn serialize_none(self) -> Checked {
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Checked {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Checked {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Checked {
        Ok(())
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Checked {
        Ok(())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(self, _: &'static str, v: &T) -> Checked {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        v: &T,
    ) -> Checked {
        v.serialize(self)
    }
    fn serialize_seq(self, _: Option<usize>) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
    fn serialize_map(self, _: Option<usize>) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Self, Refused> {
        Ok(self)
    }
}

impl ser::SerializeSeq for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}

impl ser::SerializeTuple for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}

// A key becomes a JSON string, whatever its type: only values are numbers.
impl ser::SerializeMap for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, _: &T) -> Checked {
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}

impl ser::SerializeStruct for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, _: &'static str, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}

impl ser::SerializeStructVariant for Check {
    type Ok = ();
    type Error = Refused;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, _: &'static str, v: &T) -> Checked {
        v.serialize(Check)
    }
    fn end(self) -> Checked {
        Ok(())
    }
}
