import { utils } from "../gen/substantial.js";

export function print(message) {
    console.log(message);
}

export function eprint(message) {
    console.error(message);
}

export function indirectCall(callee, args) {
    const definitions = {
        concat(items) {
            return new Promise((resolve, _reject) => {
                setTimeout(() => {
                    resolve("dsadsasadsadas")
                }, 1000);
            });
        }
    };
    setTimeout(() => {
        utils.hostResult(callee, args.join(""))
    }, 5000);


    // definitions[callee](args)
    //     .then((res) => { console.log("S"); utils.hostResult(callee, JSON.stringify(res)) })
    //     .catch((err) => { console.log("S"); utils.hostError(callee, JSON.stringify(err)) } );
}