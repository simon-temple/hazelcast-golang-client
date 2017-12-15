package hz

import (
	"encoding/binary"
)

/*
This is a reverse engineer of the hz java code as two golang mummur3 projects I tried did not consistently produce the same results!
For example, consider the key: []byte{0, 0, 0, 5, 't', 'm', 'p', '.', '4'}
This code produces a negative hash, both https://github.com/reusee/mmh3 and https://github.com/spaolacci/murmur3 produce the same positive hash !!
 */
func hash32(key []byte, seed int32) int32 {

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

func CalcHash(connection *ClientConnection, key [] byte) int32 {

	// To determine the partition ID of an operation, compute the Murmur Hash (version 3, 32-bit, see https://en.wikipedia.org/wiki/MurmurHash and http s://code.google.com/p/smhasher/wiki/MurmurHash3)
	// of a certain byte-array (which is identified for each message description section) and take the modulus of the result over the total number of partitions. The seed for the Murmur Hash SHOULD
	// be 0x01000193. Most operations with a key parameter use the key parameter byte-array as the data for the hash calculation.

	av := hash32(key, 0x01000193)

	if av == INTEGER32_MIN_VALUE {
		av = 0
	} else {
		if av < 0 {
			av = -av
		}
	}
	hash := int32(av % connection.partitionCount)

	connection.Logger.Trace("### Hash Calc: murmur3: %d, partition count: %d, hash: %d", av, connection.partitionCount, hash)

	return hash
}