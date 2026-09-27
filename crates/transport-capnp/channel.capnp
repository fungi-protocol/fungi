@0xeacf7a61dba65b94;

struct Result(T, E) {
  union {
    ok @0 :T;
    err @1 :E;
  }
}

struct Unit {}

struct SendFailure {
  union {
    tooLarge @0 :UInt64;
    closed @1 :Void;
    failed @2 :Text;
  }
}

struct RecvFailure {
  union {
    closed @0 :Void;
    failed @1 :Text;
  }
}

interface Channel(M, SendE, RecvE) {
  send @0 (message :M) -> (result :Result(Unit, SendE));
  recv @1 () -> (result :Result(M, RecvE));
}

