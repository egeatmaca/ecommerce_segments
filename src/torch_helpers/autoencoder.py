import torch
import torch.nn as nn
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader
import numpy as np


class AutoEncoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dims: list[int], latent_dim: int) -> None:
        super().__init__()
        encoder_layers, decoder_layers = self.make_layers(input_dim, hidden_dims, latent_dim) 
        self.encoder = nn.Sequential(*encoder_layers)
        self.decoder = nn.Sequential(*decoder_layers)

    @staticmethod
    def make_layers(input_dim: int, hidden_dims: list[int], latent_dim: int) -> list[nn.Module]:
        dims = [input_dim] 
        if type(hidden_dims) == list:
            dims += hidden_dims
        if type(hidden_dims) == int:
            dims += [hidden_dims]
        dims += [latent_dim]

        encoder_layers = []
        decoder_layers = []
        
        n_linear_layers = len(dims)
        last_dim = None
        for i, dim in enumerate(dims):
            if i > 0:
                encoder_layers.append(nn.Linear(last_dim, dim))
                decoder_layers.append(nn.Linear(dim, last_dim))
                if i < n_linear_layers - 1:
                    encoder_layers.append(nn.ReLU())
                    decoder_layers.append(nn.ReLU())

            last_dim = dim

        decoder_layers = reversed(decoder_layers)  

        return encoder_layers, decoder_layers

    def forward(self, x: torch.Tensor) -> (torch.Tensor, torch.Tensor):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return encoded, decoded


class AETrainer:
    def __init__(self, 
                 model: nn.Module,
                 criterion: nn.modules.loss._Loss = nn.MSELoss(),
                 optimizer_class: torch.optim.Optimizer = torch.optim.Adam,
                 lr: float = 0.001,
                 lr_reduce_factor: float = 0.5,
                 lr_reduce_patience: int = 3):
        self.device = self.get_device()
        self.model = model.to(self.device)
        self.criterion = criterion
        self.optimizer = optimizer_class(model.parameters(), lr=lr)
        self.scheduler = ReduceLROnPlateau(self.optimizer, 'min', factor=0.5, patience=3)

    @staticmethod
    def get_device():
        has_mps = torch.backends.mps.is_built()
        device = "mps" if has_mps else "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {device}")
        return device

    def train(self, train_loader: DataLoader, test_loader: DataLoader, 
              epochs: int = 1000, early_stop_threshold: int = 5):
        early_stop_count = 0
        min_val_loss = float('inf')

        for epoch in range(epochs):
            # Training
            self.model.train()
            for batch in train_loader:
                x_batch = batch[0]
                x_batch.to(self.device)
                self.optimizer.zero_grad()
                encoded, decoded = self.model(x_batch)
                loss = self.criterion(decoded, x_batch)
                loss.backward()
                self.optimizer.step()

            # Validation
            self.model.eval()
            val_losses = []
            with torch.no_grad():
                for batch in test_loader:
                    x_batch = batch[0]
                    x_batch.to(self.device)
                    encoded, decoded = self.model(x_batch)
                    loss = self.criterion(decoded, x_batch)
                    val_losses.append(loss.item())

            val_loss = np.mean(val_losses)
            self.scheduler.step(val_loss)

            if val_loss < min_val_loss:
                min_val_loss = val_loss
                early_stop_count = 0
            else:
                early_stop_count += 1

            if early_stop_count >= early_stop_threshold:
                print("Early stopping!")
                break
            print(f"Epoch {epoch + 1}/{epochs}, Validation Loss: {val_loss:.9f}")

        return min_val_loss
