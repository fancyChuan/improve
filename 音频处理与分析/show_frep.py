import numpy as np
import matplotlib.pyplot as plt
import sounddevice as sd
import soundfile as sf


def plot_spec(y, sr, block_size=2048, hop_size=512):
    fig, ax = plt.subplots()

    # Create time array
    times = np.arange(len(y)) / float(sr)

    # Setup the spectrogram
    f, t, spec = plt.specgram(y, NFFT=block_size, Fs=sr, window=np.hanning(block_size),
                              noverlap=hop_size, cmap='viridis', xextent=(0, times[-1]))

    # Set the limits of the plot to the limits of the data
    ax.set_ylim([0, sr / 2])
    ax.set_xlim([times[0], times[-1]])

    # Set the x and y axis labels
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Frequency (Hz)')

    # Show the plot
    plt.show(block=False)

    return ax


def callback(indata, frames, time, status):
    # Remove any DC component
    x = indata[:, 0] - np.mean(indata[:, 0])

    # Compute the spectrogram of the data
    spec = np.abs(np.fft.rfft(x))
    freq = np.fft.rfftfreq(len(x), d=1.0 / fs)

    # Update the plot with the new spectrogram data
    line.set_xdata(time)
    line.set_ydata(freq)
    im.set_data(20 * np.log10(spec.reshape(-1, 1)))

    # Redraw the spectrogram
    fig.canvas.draw()


# Load the audio file
filename = 'mp3/Ahxello-Infinity.mp3'
y, fs = sf.read(filename)

# Create the plot
ax = plot_spec(y, fs)

# Setup the audio stream
block_size = 2048
stream = sd.InputStream(callback=callback, blocksize=block_size, samplerate=fs, channels=1)

# Start the audio stream
with stream:
    while True:
        pass
