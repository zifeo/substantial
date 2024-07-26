wit_bindgen::generate!({ world: "substantial" });

struct Substantial;

impl Guest for Substantial {
  fn process_save(events: String) {
    unreachable!();
  }

  fn process_sleep(events: String) {
    unreachable!();
  }

  fn next_id(ctx_id: String) {
    unreachable!();
  }
}

export!(Substantial);