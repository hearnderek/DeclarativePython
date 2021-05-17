def run(self, etl, f, h, g, valuation, print_g, print_f_after_g, input_value_, t_, g_, print_g_, f_, print_f_after_g_, valuation_, etl_, h_):
    etl_[0] = etl()
    for i in range(1,1):
        etl_[i] = etl_[0]
    f_[0] = f()
    for i in range(1,1):
        f_[i] = f_[0]
    h_[0] = h(input_value_[0])
    for i in range(1,1):
        h_[i] = h_[0]
    g_[0] = g(f_[0])
    for i in range(1,1):
        g_[i] = g_[0]
    valuation_[0] = valuation(etl_[0])
    for i in range(1,1):
        valuation_[i] = valuation_[0]
    print_g_[0] = print_g(g_[0])
    for i in range(1,1):
        print_g_[i] = print_g_[0]
    print_f_after_g_[0] = print_f_after_g(f_[0], print_g_[0])
    for i in range(1,1):
        print_f_after_g_[i] = print_f_after_g_[0]
