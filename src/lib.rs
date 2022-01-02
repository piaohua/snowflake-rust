//! +--------------------------------------------------------------------------+
//! | 1 Bit Unused | 41 Bit Timestamp |  10 Bit NodeID  |   12 Bit Sequence ID |
//! +--------------------------------------------------------------------------+
//! ID Format
//! By default, the ID format follows the original Twitter snowflake format.
//! The ID as a whole is a 63 bit integer stored in an int64
//! 41 bits are used to store a timestamp with millisecond precision, using a custom epoch.
//! 10 bits are used to store a node id - a range from 0 through 1023.
//! 12 bits are used to store a sequence number - a range from 0 through 4095.

use chrono::{TimeZone, Local, DateTime};
use rand::Rng;
use std::thread;
use std::time::Duration;

// SEQUENCE_BITS 自增量占用比特
const SEQUENCE_BITS:i32 = 12;
// WORKER_ID_BITS 工作进程ID比特
const WORKER_ID_BITS:i32 = 5;
// DATA_CENTER_ID_BITS 数据中心ID比特
const DATA_CENTER_ID_BITS:i32 = 5;
// NODE_ID_BITS 节点ID比特
const NODE_ID_BITS:i32 = DATA_CENTER_ID_BITS + WORKER_ID_BITS;
// SEQUENCE_MASK 自增量掩码（最大值）
const SEQUENCE_MASK:i32 = -1 ^ (-1 << SEQUENCE_BITS);
// DATA_CENTER_ID_LEFT_SHIFT_BITS 数据中心ID左移比特数（位数）
const DATA_CENTER_ID_LEFT_SHIFT_BITS:i32 = WORKER_ID_BITS + SEQUENCE_BITS;
// WORKER_ID_LEFT_SHIFT_BITS 工作进程ID左移比特数（位数）
const WORKER_ID_LEFT_SHIFT_BITS:i32 = SEQUENCE_BITS;
// NODE_ID_LEFT_SHIFT_BITS 节点ID左移比特数（位数）
const NODE_ID_LEFT_SHIFT_BITS:i32 = DATA_CENTER_ID_BITS + WORKER_ID_BITS + SEQUENCE_BITS;
// TIMESTAMP_LEFT_SHIFT_BITS 时间戳左移比特数（位数）
const TIMESTAMP_LEFT_SHIFT_BITS:i32 = NODE_ID_LEFT_SHIFT_BITS;
// WORKER_ID_MAX 工作进程ID最大值
const WORKER_ID_MAX:i32 = -1 ^ (-1 << WORKER_ID_BITS);
// DATA_CENTER_ID_MAX 数据中心ID最大值
const DATA_CENTER_ID_MAX:i32 = -1 ^ (-1 << DATA_CENTER_ID_BITS);
// NODE_ID_MAX 节点ID最大值
const NODE_ID_MAX:i32 = -1 ^ (-1 << NODE_ID_BITS);

/// ID an uint64 of the snowflake ID
#[derive(Debug)]
pub struct ID(u64);

impl ID {
    // new returns a new snowflake id
    pub fn new(node_id: i32) -> ID {
        let mut node: Node = Node::new(node_id);
        node.generate_id()
    }

    // time returns an int64 unix timestamp in milliseconds of the snowflake ID time
    pub fn time(&self) -> i64 {
        let ts = (self.0 as u64) >> TIMESTAMP_LEFT_SHIFT_BITS;
        let epoch = Local.ymd(2022, 1, 1).and_hms(0, 0, 0);
        epoch.timestamp_millis() + (ts as i64)
    }
    
    // node returns an uint64 of the snowflake ID node number
    pub fn node(&self) -> u64 {
        // return (uint64(i) >> workeridLeftShiftBits) & (-1 ^ (-1 << nodeidBits))
        (self.0 >> WORKER_ID_LEFT_SHIFT_BITS) & (NODE_ID_MAX as u64)
    }
    
    // sequence returns an uint64 of the snowflake sequence number
    pub fn sequence(&self) -> u64 {
        self.0 & (SEQUENCE_MASK as u64)
    }
}

/// Node a node struct for a snowflake generator
#[derive(Debug)]
pub struct Node {
    timestamp: i64,
    datacenter_id: i32,
    worker_id: i32,
    sequence: i32,
    epoch: DateTime<Local>,
}

impl Node {
    // new returns a new snowflake node that can be used to generate snowflake
    pub fn new(node_id: i32) -> Node {
        if node_id >= NODE_ID_MAX {
            eprintln!("invalid node_id:{}", node_id); 
            panic!("invalid node_id:{}", node_id);
        }

        let datacenter_id:i32 = node_id >> DATA_CENTER_ID_BITS;
        let worker_id:i32 = node_id & (-1 ^ (-1 << WORKER_ID_BITS));

        if datacenter_id >= DATA_CENTER_ID_MAX {
        	eprintln!("invalid datacenter_id:{}", datacenter_id);
        	panic!("invalid datacenter_id:{}", datacenter_id);
        }
        if worker_id >= WORKER_ID_MAX {
        	eprintln!("invalid worker_id:{}", worker_id);
        	panic!("invalid worker_id:{}", worker_id);
        }

        let node: Node = Node{
            timestamp:0, 
            datacenter_id: datacenter_id,
            worker_id: worker_id,
            sequence:0,
            // epoch 时间偏移量，从2022年1月1日零点开始
            epoch: Local.ymd(2022, 1, 1).and_hms(0, 0, 0),
        };

        node
    }

    // generate_id creates and returns a unique snowflake ID
    fn generate_id(&mut self) -> ID {
        let id: u64 = self.generate();
        ID(id)
    }

    // generate creates and returns a unique snowflake ID
    // ID生成器,长度为64bit,从高位到低位依次为
    // 1bit   符号位
    // 41bits 时间偏移量从2019年6月16日零点到现在的毫秒数
    // 10bits 节点工作进程ID
    // 12bits 同一个毫秒内的自增量
    pub fn generate(&mut self) -> u64 {
        let now:i64 = Local::now().signed_duration_since(self.epoch).num_milliseconds();
        println!("now {}", now);
        if now == self.timestamp {
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK;
            if self.sequence == 0 {
	            //保证当前时间大于最后时间。时间回退会导致产生重复id
	            self.wait();
            }
        } else {
            self.init_sequence();
        }
	    //设置最后时间偏移量
	    self.timestamp = now;
	    self.id()
    }

    // 避免在跨毫秒时序列号总是归0
    fn init_sequence(&mut self) {
        let mut rng = rand::thread_rng();
        self.sequence = rng.gen_range::<i32, _>(0..10);
    }

    // 生成id
    fn id(&self) -> u64 {
        (self.timestamp as u64) << TIMESTAMP_LEFT_SHIFT_BITS |
        (self.datacenter_id as u64) << DATA_CENTER_ID_LEFT_SHIFT_BITS |
        (self.worker_id as u64) << WORKER_ID_LEFT_SHIFT_BITS |
        (self.sequence as u64)
    }

    // 不停获得时间，直到大于最后时间
    fn wait(&self) {
        loop {
            let now:i64 = Local::now().signed_duration_since(self.epoch).num_milliseconds();
            if now > self.timestamp {
                break
            } else if now == self.timestamp {
                thread::sleep(Duration::from_millis(1));
                continue
            }
            thread::sleep(Duration::from_millis((self.timestamp - now) as u64));
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    #[test]
    fn node_test() {
        use super::Node;
        let mut node: Node = Node::new(460);
        println!("{:?}", node);
        let id:u64 = node.generate();
        println!("id = {}", id);
    }

    #[test]
    fn id_test() {
        use super::ID;
        use chrono::{Local, TimeZone};
        let id: ID = ID::new(460);
        println!("{:?}", id);
        let timestamp:i64 = id.time();
        println!("timestamp = {}", timestamp);
        let dt = Local.timestamp_millis(timestamp).to_rfc3339();
        println!("datetime = {}", dt);
        let node_id:u64 = id.node();
        println!("node_id = {}", node_id);
        let sequence:u64 = id.sequence();
        println!("sequence = {}", sequence);
    }
}
