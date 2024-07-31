export function print(message) {
    console.log(message);
}

export function eprint(message) {
    console.error(message);
}

export const globalTasks = {};
function define(name, args, asyncFn) {
    if (globalTasks[name]) {
        throw new Error(`fatal: another task of the same name (${name}) is pending`);
    }
    globalTasks[name] = { asyncFn, args };
    return { refHost: name };
}

export function concat(items) {
    return define("concat", [items], (items) => {
        return new Promise((resolve, _reject) => {
            setTimeout(() => {
                resolve(items.join(""))
            }, 1000);
        })
    });
}
