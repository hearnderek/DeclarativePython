def f0(t, f0):
    if t <= 0:
        return 1
    else:
        return f0[t-1] + 1



def f1(t, f0):
    return f0[t] + 1



def f2(t, f1):
    return f1[t] + 1



def f3(t, f2):
    return f2[t] + 1



def f4(t, f3):
    return f3[t] + 1



def f5(t, f4):
    return f4[t] + 1



def f6(t, f5):
    return f5[t] + 1



def f7(t, f6):
    return f6[t] + 1



def f8(t, f7):
    return f7[t] + 1



def f9(t, f8):
    return f8[t] + 1



def f10(t, f9):
    return f9[t] + 1



def f11(t, f10):
    return f10[t] + 1



def f12(t, f11):
    return f11[t] + 1



def f13(t, f12):
    return f12[t] + 1



def f14(t, f13):
    return f13[t] + 1



def f15(t, f14):
    return f14[t] + 1



def f16(t, f15):
    return f15[t] + 1



def f17(t, f16):
    return f16[t] + 1



def f18(t, f17):
    return f17[t] + 1



def f19(t, f18):
    return f18[t] + 1



def f20(t, f19):
    return f19[t] + 1



def f21(t, f20):
    return f20[t] + 1



def f22(t, f21):
    return f21[t] + 1



def f23(t, f22):
    return f22[t] + 1



def f24(t, f23):
    return f23[t] + 1



def f25(t, f24):
    return f24[t] + 1



def f26(t, f25):
    return f25[t] + 1



def f27(t, f26):
    return f26[t] + 1



def f28(t, f27):
    return f27[t] + 1



def f29(t, f28):
    return f28[t] + 1



def f30(t, f29):
    return f29[t] + 1



def f31(t, f30):
    return f30[t] + 1



def f32(t, f31):
    return f31[t] + 1



def f33(t, f32):
    return f32[t] + 1



def f34(t, f33):
    return f33[t] + 1



def f35(t, f34):
    return f34[t] + 1



def f36(t, f35):
    return f35[t] + 1



def f37(t, f36):
    return f36[t] + 1



def f38(t, f37):
    return f37[t] + 1



def f39(t, f38):
    return f38[t] + 1



def f40(t, f39):
    return f39[t] + 1



def f41(t, f40):
    return f40[t] + 1



def f42(t, f41):
    return f41[t] + 1



def f43(t, f42):
    return f42[t] + 1



def f44(t, f43):
    return f43[t] + 1



def f45(t, f44):
    return f44[t] + 1



def f46(t, f45):
    return f45[t] + 1



def f47(t, f46):
    return f46[t] + 1



def f48(t, f47):
    return f47[t] + 1



def f49(t, f48):
    return f48[t] + 1




if __name__ == '__main__':
    import pandas as pd
    import numpy as np
    from pathlib import Path
    import context
    import declarative
    current_file = Path(__file__).stem
    timesteps = 100
    for optimization in range(0, 6):
        df = pd.DataFrame()
        declarative.turn_off_progress_bar = True
        ie = declarative.IterativeEngine(df, t=timesteps, display_progressbar=True)
        ie.calculate(1, optimization)
        #print(ie.engine.results)
        df = ie.results_to_df()
        print(df)

        for col in df.columns:
            assert df.isna().sum()[col] == 0, f"in optimization {optimization} -- {col} is has pd.NA values"