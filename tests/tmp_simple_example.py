def run(self, etl, f, h, g, valuation, print_g, print_f_after_g, input_value_, t_, h_, valuation_, print_f_after_g_, print_g_, f_, etl_, g_):
    etl_ = [etl()] * 1
    f_ = [f()] * 1
    h_ = [h(input_value_[0])] * 1
    g_ = [g(f_[0])] * 1
    valuation_ = [valuation(etl_[0])] * 1
    print_g_ = [print_g(g_[0])] * 1
    print_f_after_g_ = [print_f_after_g(f_[0], print_g_[0])] * 1
