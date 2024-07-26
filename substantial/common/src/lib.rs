wit_bindgen::generate!({
    world: "host",
});

struct MyHost;

impl Guest for MyHost {
    fn run() {
        println!("Hello, world! from stdout from rust");
        let cwd = std::env::current_dir().expect("not fail");
        println!("cwd is at {}", cwd.to_string_lossy());

        if let Ok(dirs) = std::fs::read_dir(cwd) {
            for (i, d) in dirs.map(|d| d.unwrap()).enumerate() {
                println!(" #{i} => {d:?}");
                if i >= 4 {
                    break;
                }
            }
            println!(" ..etc")
        }
    }

    fn set_redis(host: String, port: u32, password: Option<String>) {
        /*
        error[E0412]: cannot find type `c_int` in module `sys`
   --> /home/afmika/.cargo/registry/src/index.crates.io-6f17d22bba15001f/socket2-0.5.7/src/lib.rs:650:45
    |
650 |     pub fn with_flags(mut self, flags: sys::c_int) -> Self {
    |                                             ^^^^^ not found in `sys`

         */

        // use reqwest::Client;
        // println!("info: {host}:{port}");
        // let client = Client::new();
        // let req = client
        // .get("http://example.com")
        // .send();
        // let res = req.type_id();
        // println!("http: {res:?}")
        // let response = isahc::get("https://example.org").unwrap();
        // println!("http: {response:?}")
    }

}

export!(MyHost);