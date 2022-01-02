# snowflake

    This sample is a very simple Twitter snowflake generator in [Rust](https://www.rust-lang.org/)

## ID Format
By default, the ID format follows the original Twitter snowflake format.
* The ID as a whole is a 63 bit integer stored in an int64
* 41 bits are used to store a timestamp with millisecond precision, using a custom epoch.
* 10 bits are used to store a node id - a range from 0 through 1023.
* 12 bits are used to store a sequence number - a range from 0 through 4095.

### How it Works.
Each time you generate an ID, it works, like this.
* A timestamp with millisecond precision is stored using 41 bits of the ID.
* Then the NodeID is added in subsequent bits.
* Then the Sequence Number is added, starting at 0 and incrementing for each ID generated in the same millisecond. If you generate enough IDs in the same millisecond that the sequence would roll over or overfill then the generate function will pause until the next millisecond.

The default Twitter format shown below.
```
+--------------------------------------------------------------------------+
| 1 Bit Unused | 41 Bit Timestamp |  10 Bit NodeID  |   12 Bit Sequence ID |
+--------------------------------------------------------------------------+
```

Using the default settings, this allows for 4096 unique IDs to be generated every millisecond, per Node ID.

## Installation

```toml
snowflake = { git = "https://github.com/piaohua/snowflake-rust", version = "0.1.0" }
```

## Usage:

```sh
# run test
> cargo test -- --nocapture
```

## References:
*  https://github.com/bwmarrin/snowflake
*  https://tech.meituan.com/2017/04/21/mt-leaf.html
*  https://segmentfault.com/a/1190000011282426
