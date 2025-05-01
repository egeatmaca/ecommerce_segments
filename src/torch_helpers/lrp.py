import torch
from torch import nn
from copy import deepcopy


def relevance_filter(r: torch.tensor, top_k_percent: float = 1.0) -> torch.tensor:
    """Filter that allows largest k percent values to pass for each batch dimension.

    Filter keeps k% of the largest tensor elements. Other tensor elements are set to
    zero. Here, k = 1 means that all relevance scores are passed on to the next layer.

    Args:
        r: Tensor holding relevance scores of current layer.
        top_k_percent: Proportion of top k values that is passed on.

    Returns:
        Tensor of same shape as input tensor.

    """
    assert 0.0 < top_k_percent <= 1.0

    if top_k_percent < 1.0:
        size = r.size()
        r = r.flatten(start_dim=1)
        num_elements = r.size(-1)
        k = max(1, int(top_k_percent * num_elements))
        top_k = torch.topk(input=r, k=k, dim=-1)
        r = torch.zeros_like(r)
        r.scatter_(dim=1, index=top_k.indices, src=top_k.values)
        return r.view(size)
    else:
        return r

class RelevancePropagationLinear(nn.Module):
    """Layer-wise relevance propagation for linear transformation.

    Optionally modifies layer weights according to propagation rule. Here z^+-rule

    Attributes:
        layer: linear transformation layer.
        eps: a value added to the denominator for numerical stability.

    """

    def __init__(
        self,
        layer: torch.nn.Linear,
        mode: str = "z_plus",
        eps: float = 1.0e-05,
        top_k: float = 0.0,
    ) -> None:
        super().__init__()

        self.layer = layer

        if mode == "z_plus":
            self.layer.weight = torch.nn.Parameter(self.layer.weight.clamp(min=0.0))
            self.layer.bias = torch.nn.Parameter(torch.zeros_like(self.layer.bias))

        self.eps = eps
        self.top_k = top_k

    @torch.no_grad()
    def forward(self, a: torch.tensor, r: torch.tensor) -> torch.tensor:
        if self.top_k:
            r = relevance_filter(r, top_k_percent=self.top_k)
        z = self.layer.forward(a) + self.eps
        s = r / z
        c = torch.mm(s, self.layer.weight)
        r = (a * c).data
        return r


class RelevancePropagationReLU(nn.Module):
    def __init__(self, layer: torch.nn.ReLU, top_k: float = 0.0) -> None:
        super().__init__()

    @torch.no_grad()
    def forward(self, a: torch.tensor, r: torch.tensor) -> torch.tensor:
        return r


class LinearLRP(nn.Module):
    """Class wraps PyTorch model to perform layer-wise relevance propagation."""

    LAYERS_LOOKUP = {
        torch.nn.modules.linear.Linear: RelevancePropagationLinear,
        torch.nn.modules.activation.ReLU: RelevancePropagationReLU,
    }

    def __init__(self, model: torch.nn.Module, top_k: float = 0.0) -> None:
        super().__init__()
        self.model = model
        self.top_k = top_k

        self.model.eval()  # self.model.train() activates dropout / batch normalization etc.!

        # Parse network
        self.layers = self._get_layer_operations()

        # Create LRP network
        self.lrp_layers = self._create_lrp_model()

    def _create_lrp_model(self) -> torch.nn.ModuleList:
        """Method builds the model for layer-wise relevance propagation.

        Returns:
            LRP-model as module list.

        """
        # Clone layers from original model. This is necessary as we might modify the weights.
        layers = deepcopy(self.layers)

        # Run backwards through layers
        for i, layer in enumerate(layers[::-1]):
            try:
                layers[i] = self.LAYERS_LOOKUP[layer.__class__](layer=layer, top_k=self.top_k)
            except KeyError:
                message = (
                    f"Layer-wise relevance propagation not implemented for "
                    f"{layer.__class__.__name__} layer."
                )
                raise NotImplementedError(message)

        return layers

    def _get_layer_operations(self) -> torch.nn.ModuleList:
        """Get all network operations and store them in a list.

        This method is adapted to VGG networks from PyTorch's Model Zoo.
        Modify this method to work also for other networks.

        Returns:
            Layers of original model stored in module list.

        """
        layers = torch.nn.ModuleList()

        for sequential in self.model.modules(): 
            if not isinstance(sequential, torch.nn.Sequential):
                continue

            for layer in sequential.modules(): 
                if isinstance(layer, torch.nn.Sequential):
                    continue
                
                layers.append(layer)

        return layers

    def forward(self, x: torch.tensor) -> torch.tensor:
        """Forward method that first performs standard inference followed by layer-wise relevance propagation.

        Args:
            x: Input tensor

        Returns:
            Tensor holding relevance scores of input

        """
        activations = list()

        # Run inference and collect activations.
        with torch.no_grad():
            # Replace image with ones avoids using image information for relevance computation.
            activations.append(torch.ones_like(x))
            for layer in self.layers:
                x = layer.forward(x)
                activations.append(x)

        # Reverse order of activations to run backwards through model
        activations = activations[::-1]
        activations = [a.data.requires_grad_(True) for a in activations]

        # Initial relevance scores are the network's output activations
        relevance = torch.softmax(activations.pop(0), dim=-1)  # Unsupervised

        # Perform relevance propagation
        for i, layer in enumerate(self.lrp_layers):
            relevance = layer.forward(activations.pop(0), relevance)

        return relevance.detach().cpu()
