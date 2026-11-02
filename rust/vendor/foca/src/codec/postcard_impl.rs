/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use bytes::{Buf, BufMut};
use postcard::ser_flavors::Flavor as SerFlavor;

use crate::{Codec, Header, Member};

/// `PostcardCodec` encodes/decodes packets using [`postcard`].
#[derive(Debug, Clone, Copy)]
pub struct PostcardCodec;

// XXX We can use Buf::chunk here because Foca guarantees the buffer is
//     contiguous... Maybe a marker trait like `trait ContiguousBuf: Buf {}`
//     or some other form of making it explicit in the type would be
//     helpful?
impl<T> Codec<T> for PostcardCodec
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    type Error = postcard::Error;

    fn encode_header(&mut self, payload: &Header<T>, buf: impl BufMut) -> Result<(), Self::Error> {
        postcard::serialize_with_flavor(payload, WrappedBuf(buf))
    }

    fn decode_header(&mut self, mut buf: impl Buf) -> Result<Header<T>, Self::Error> {
        let remaining = buf.remaining();
        debug_assert_eq!(remaining, buf.chunk().len());
        let (payload, rest) = postcard::take_from_bytes(buf.chunk())?;
        let after = rest.len();
        buf.advance(remaining - after);
        Ok(payload)
    }

    fn encode_member(&mut self, member: &Member<T>, buf: impl BufMut) -> Result<(), Self::Error> {
        postcard::serialize_with_flavor(member, WrappedBuf(buf))
    }

    fn decode_member(&mut self, mut buf: impl Buf) -> Result<Member<T>, Self::Error> {
        let remaining = buf.remaining();
        debug_assert_eq!(remaining, buf.chunk().len());
        let (member, rest) = postcard::take_from_bytes(buf.chunk())?;
        let after = rest.remaining();
        buf.advance(remaining - after);
        Ok(member)
    }
}

struct WrappedBuf<B>(B);

impl<B: BufMut> SerFlavor for WrappedBuf<B> {
    type Output = ();

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        if self.0.has_remaining_mut() {
            self.0.put_u8(data);
            Ok(())
        } else {
            Err(postcard::Error::SerializeBufferFull)
        }
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(())
    }

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<Self::Output> {
        if self.0.remaining_mut() >= data.len() {
            self.0.put_slice(data);
            Ok(())
        } else {
            Err(postcard::Error::SerializeBufferFull)
        }
    }
}

#[cfg(test)]
mod test {
    use super::PostcardCodec;

    #[test]
    fn postcard_roundtrip() -> Result<(), postcard::Error> {
        crate::testing::verify_codec_roundtrip(PostcardCodec)
    }

    fn enc<T: serde::Serialize>(v: &T) -> Result<alloc::vec::Vec<u8>, postcard::Error> {
        let mut buf = [0u8; 256];
        Ok(postcard::to_slice(v, &mut buf)?.to_vec())
    }

    // stock foca 2.0.0's Member, as serde derived it
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct StockMember {
        id: u64,
        incarnation: u16,
        state: crate::State,
    }

    #[test]
    fn members_without_origin_match_stock_foca() -> Result<(), postcard::Error> {
        use crate::{Member, State};
        for state in [State::Alive, State::Suspect, State::Down] {
            let ours = enc(&Member::new(7u64, 300, state))?;
            let stock = enc(&StockMember { id: 7, incarnation: 300, state })?;
            assert_eq!(stock, ours);
            assert_eq!(Member::new(7u64, 300, state), postcard::from_bytes(&stock)?);
        }
        Ok(())
    }

    #[test]
    fn suspicion_origin_roundtrip() -> Result<(), postcard::Error> {
        use crate::Member;
        let m = Member::suspect_by(7u64, 3, 9u64);
        let bytes = enc(&m)?;
        let back: Member<u64> = postcard::from_bytes(&bytes)?;
        assert_eq!(m, back);
        assert_eq!(Some(&9), back.suspected_by());
        // and a list of them, as restore_membership's file holds
        let list = alloc::vec![Member::alive(1u64), m, Member::down(2u64)];
        let back: alloc::vec::Vec<Member<u64>> = postcard::from_bytes(&enc(&list)?)?;
        assert_eq!(list, back);
        Ok(())
    }
}
