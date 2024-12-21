# plot the model output

import matplotlib.pyplot as plt
import seaborn as sns

def plot_and_save(x, y, out_file):
    sns.set(style='whitegrid')
    sns.lineplot(x=x, y=y, color='green', linewidth=2.5)

    plt.ylabel('predicted')
    plt.xlabel('input')

    plt.savefig(out_file)
    # plt.show()
