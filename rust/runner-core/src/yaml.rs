//! Bound expanded YAML while deserializing, before anchors can allocate a large
//! object tree. Limits include aliases, keys, tags and nested collection nodes.
use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde_yaml::{
    Mapping, Value,
    value::{Tag, TaggedValue},
};

struct Budget {
    nodes: usize,
    bytes: usize,
}
impl Budget {
    fn spend<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| E::custom("expanded YAML exceeds its memory limit"))?;
        Ok(())
    }
}
struct Seed<'a> {
    budget: &'a mut Budget,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        if self.depth > 96 || self.budget.nodes == 0 {
            return Err(de::Error::custom(
                "YAML is too deeply nested or has too many values",
            ));
        }
        self.budget.nodes -= 1;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a bounded YAML value")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_some<D: de::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        self.deserialize(d)
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        self.budget.spend(v.len())?;
        Ok(Value::String(v.into()))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        self.budget.spend(v.len())?;
        Ok(Value::String(v))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut out = Vec::new();
        while let Some(value) = sequence.next_element_seed(Seed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            out.push(value);
        }
        Ok(Value::Sequence(out))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut mapping: A) -> Result<Value, A::Error> {
        let mut out = Mapping::new();
        while let Some(key) = mapping.next_key_seed(Seed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            if out.contains_key(&key) {
                return Err(de::Error::custom("duplicate YAML key"));
            }
            let value = mapping.next_value_seed(Seed {
                budget: &mut *self.budget,
                depth: self.depth + 1,
            })?;
            out.insert(key, value);
        }
        Ok(Value::Mapping(out))
    }
    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Value, A::Error> {
        let (tag, contents) = data.variant::<String>()?;
        self.budget.spend(tag.len())?;
        let value = contents.newtype_variant_seed(Seed {
            budget: self.budget,
            depth: self.depth + 1,
        })?;
        Ok(Value::Tagged(Box::new(TaggedValue {
            tag: Tag::new(tag),
            value,
        })))
    }
}
pub fn parse(source: &str) -> Result<Value, serde_yaml::Error> {
    if source.len() > 4 << 20 {
        return Err(de::Error::custom("YAML source exceeds its size limit"));
    }
    Seed {
        budget: &mut Budget {
            nodes: 65_536,
            bytes: 8 << 20,
        },
        depth: 0,
    }
    .deserialize(serde_yaml::Deserializer::from_str(source))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_compose_anchors_and_rejects_excessive_expansion() {
        let mut value = parse("x-default: &default { image: busybox, command: [sleep, infinity] }\nservices:\n  worker:\n    <<: *default\n").unwrap();
        value.apply_merge().unwrap();
        assert_eq!(
            value["services"]["worker"]["image"],
            Value::String("busybox".into())
        );
        let source = format!(
            "a: &a {}\nb: [{}]\n",
            "x".repeat(100_000),
            vec!["*a"; 100].join(",")
        );
        assert!(
            parse(&source)
                .unwrap_err()
                .to_string()
                .contains("memory limit")
        );
        assert!(parse(&format!("{}0{}", "[".repeat(100), "]".repeat(100))).is_err());
        assert!(parse("a: 1\na: 2\n").is_err());
    }
}
