import torch
import torch.nn as nn
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader
import numpy as np


class EncodingClassifier(nn.Module):
    def __init__(self, 
                 encoder: nn.Module, 
                 output_dim: int, 
                 hidden_dims: list[int] = [], 
                 freeze_encoder: bool = True) -> None:
        super().__init__()
        self.encoder = encoder

        if freeze_encoder:
            self.set_requires_grad(self.encoder, False)
        
        decoder_layers = self.make_decoder_layers(encoder, hidden_dims, output_dim)
        self.decoder = nn.Sequential(*decoder_layers)

    @staticmethod
    def make_decoder_layers(encoder: nn.Module, 
                            hidden_dims: list[int], 
                            output_dim: int) -> list[nn.Module]:
        decoder_layers = []
        input_dim = encoder[-1].out_features
        for layer_dim in hidden_dims:
            decoder_layers.append(nn.Linear(input_dim, layer_dim))
            # decoder_layers.append(nn.ReLU())
            input_dim = layer_dim
        decoder_layers.append(nn.Linear(input_dim, output_dim))
        decoder_layers.append(nn.Softmax())
        return decoder_layers

    @staticmethod
    def set_requires_grad(nn_module: nn.Module, requires_grad: bool):
        for param in nn_module.parameters():
            param.requires_grad = requires_grad

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.encoder(x)
        x = self.decoder(x)
        return x 

class ClfTrainer:
    def __init__(self, 
                 model: nn.Module,
                 criterion: nn.modules.loss._Loss = nn.CrossEntropyLoss(),
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
                x_batch, y_batch = batch[0], batch[1]
                x_batch.to(self.device)
                y_batch.to(self.device)
                self.optimizer.zero_grad()
                y_pred = self.model(x_batch)
                loss = self.criterion(y_pred, y_batch)
                loss.backward()
                self.optimizer.step()

            # Validation
            self.model.eval()
            val_losses = []
            with torch.no_grad():
                for batch in test_loader:
                    x_batch, y_batch = batch[0], batch[1]
                    x_batch.to(self.device)
                    y_batch.to(self.device)
                    y_pred = self.model(x_batch)
                    loss = self.criterion(y_pred, y_batch)
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

