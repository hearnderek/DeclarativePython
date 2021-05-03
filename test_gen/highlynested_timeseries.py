
def f(cur, prev):
    if prev:
        # couting up across columns
        d = f'def {cur}(t, {prev}):\n' + \
            f'    return {prev}[t] + 1\n' + \
            f'\n'
    else:
        # couting up across timeseries
        d = f'def {cur}(t, {cur}):\n' + \
            f'    if t <= 0:\n' + \
            f'        return 1\n' + \
            f'    else:\n' + \
            f'        return {cur}[t-1] + 1\n' + \
            f'\n'
    return d

def fnames(n):
    if n > 0:
        return (f'f{n}', f'f{n-1}')
    else:
    	return (f'f{n}', None)


def gen(n):
    funcs = [f(*fnames(i)) for i in range(n)]
    for x in funcs:
        print(x)
        print()
 


if __name__ == '__main__':
     gen(50)

