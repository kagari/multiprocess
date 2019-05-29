use std::thread;

// multithreading merge sort
fn merge(xs: Vec<f64>, ys: Vec<f64>) -> Vec<f64> {
    match (xs.as_slice(), ys.as_slice()) {
        (xs, []) => xs.to_vec(),
        ([], ys) => ys.to_vec(),
        (xs, ys) => {
            let x = xs[0];
            let y = ys[0];
            if x <= y {
                [&[x], merge(xs[1..].to_vec(), ys.to_vec()).as_slice()].concat().to_vec()
            } else {
                [&[y], merge(xs.to_vec(), ys[1..].to_vec()).as_slice()].concat().to_vec()
            }
        }
    }
}

fn halve(xs: Vec<f64>) -> (Vec<f64>, Vec<f64>) {
    (xs[..xs.len()/2].to_vec(), xs[xs.len()/2..].to_vec())
}

fn msort(xs: Vec<f64>, n_threads: i32) -> Vec<f64> {
    match xs.as_slice() {
        [] => [].to_vec(),
        [x] => [*x].to_vec(),
        xs => {
            let (ys, zs) = halve(xs.to_vec());
            if n_threads <= 1 {
                let left = thread::spawn(move || { msort(ys.to_vec(), 0) });
                let right = thread::spawn(move || { msort(xs.to_vec(), 0) });
                merge(left.join().unwrap(), right.join().unwrap())
            } else {
                merge(msort(ys, n_threads/2), msort(zs, n_threads/2))
            }
        }
    }
}

fn qsort(xs: Vec<f64>) -> Vec<f64> {
    match xs.as_slice() {
        [] => [].to_vec(),
        xs => {
            let x = xs[0];
            let xs = xs[1..].to_vec();
            let smaller: Vec<f64> = xs.iter().cloned().filter(|a| a <= &x).collect();
            let larger: Vec<f64> = xs.iter().cloned().filter(|a| a >= &x).collect();
            [[qsort(smaller).as_slice(), &[x]].concat(), qsort(larger)].concat()
        }
    }
}

fn main() {
    let target = vec![3.,1.,4.,5.,2.];
    let n_threads = 4;
    println!("target: {:?}", target);
    println!("sorted: {:?}", msort(target, n_threads));
    let target = vec![3.,1.,4.,5.,2.];
    println!("sorted: {:?}", qsort(target));
}
