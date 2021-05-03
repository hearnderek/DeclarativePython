
def f(cur, prev):
    if prev:
        d = f'def {cur}({prev}):\n' + \
            f'    return {prev} + 1\n' + \
            f'\n'
    else:
        d = f'def {cur}():\n' + \
            f'    return 1\n' + \
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

