export function print(message) {
    console.log(message);
}

export function eprint(message) {
    console.error(message);
}

export const globalTasks = {};
function define(name, args, asyncFn) {
    globalTasks[name] = { asyncFn, args };
    return { refHost: name };
}

export function concat(items) {
    return define("concat", [items], (items) => {
        return new Promise((resolve, _reject) => {
            setTimeout(() => {
                resolve(items.join(" "))
            }, 1000);
        })
    });
}
