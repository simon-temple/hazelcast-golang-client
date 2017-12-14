package hz

import (
	"encoding/binary"
)

/**
This is a reverse engineer of the hz java code as two golang mummur3 projects I tried did not consistently produce the same results!
 */
func Hash32(key []byte, seed int32) int32 {

	length := len(key)
	if length == 0 {
		return 0
	}

	var c1, c2 int32

	c1 = -862048943
	c2 = 461845907

	nblocks := length / 4
	var h1, k1 int32

	h1 = seed

	for i := 0; i < nblocks; i++ {
		k1 = int32(binary.LittleEndian.Uint32(key[i*4:]))
		k1 *= c1
		k1 = (k1 << 15) | int32(uint32(k1) >> 17)
		k1 *= c2
		h1 ^= k1
		h1 = (h1 << 13) | int32(uint32(h1) >> 19)
		h1 = (h1 * 5) + -430675100
	}

	k1 = 0
	tailIndex := nblocks * 4

	switch length & 3 {
	case 3:
		k1 = int32(key[tailIndex+2] & 0xFF) << 16
		fallthrough
	case 2:
		k1 |= int32(key[tailIndex+1] & 0xFF) << 8
		fallthrough
	case 1:
		k1 |= int32(key[tailIndex] & 0xFF)
		k1 *= c1
		k1 = (k1 << 15) | int32(uint32(k1) >> 17)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= int32(length)
	h1 ^= int32(uint32(h1) >> 16)
	h1 *= -2048144789
	h1 ^= int32(uint32(h1) >> 13)
	h1 *= -1028477387
	h1 ^= int32(uint32(h1) >> 16)

	return h1
}

//private static <R> int MurmurHash3_x86_32(LoadStrategy<R> loader, R resource, long offset, int len, int seed) {
//// (len & ~(MURMUR32_BLOCK_SIZE - 1)) is the length rounded down to the Murmur32 block size boundary
//final long tailStart = offset + (len & ~(MURMUR32_BLOCK_SIZE - 1));
//
//int c1 = 0xcc9e2d51;
//int c2 = 0x1b873593;
//
//int h1 = seed;
//
//for (long blockAddr = offset; blockAddr < tailStart; blockAddr += MURMUR32_BLOCK_SIZE) {
//// little-endian load order
//int k1 = loader.getInt(resource, blockAddr);
//k1 *= c1;
//// ROTL32(k1,15);
//k1 = (k1 << 15) | (k1 >>> 17);
//k1 *= c2;
//
//h1 ^= k1;
//// ROTL32(h1,13);
//h1 = (h1 << 13) | (h1 >>> 19);
//h1 = h1 * 5 + 0xe6546b64;
//}
//
//// tail
//int k1 = 0;
//
//switch (len & 0x03) {
//case 3:
//k1 = (loader.getByte(resource, tailStart + 2) & 0xff) << 16;
//// fallthrough
//case 2:
//k1 |= (loader.getByte(resource, tailStart + 1) & 0xff) << 8;
//// fallthrough
//case 1:
//k1 |= loader.getByte(resource, tailStart) & 0xff;
//k1 *= c1;
//// ROTL32(k1,15);
//k1 = (k1 << 15) | (k1 >>> 17);
//k1 *= c2;
//h1 ^= k1;
//default:
//}
//
//// finalization
//h1 ^= len;
//h1 = MurmurHash3_fmix(h1);
//return h1;
//}

//public static int MurmurHash3_fmix(int k) {
//k ^= k >>> 16;
//k *= 0x85ebca6b;
//k ^= k >>> 13;
//k *= 0xc2b2ae35;
//k ^= k >>> 16;
//return k;
//}
