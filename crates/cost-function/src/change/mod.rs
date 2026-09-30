//! Change strategy.

use bitcoin::Amount;

use crate::selection::InputSelection;

/// Change selector may produce outputs but not selected scriptpubkey to along
/// with them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutputWithoutScriptpubkey(Amount);

/// What a `ChangeSelector` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeSelection {
    selected_change: Vec<OutputWithoutScriptpubkey>,
}

impl ChangeSelection {
    pub(crate) fn new(selected_change: Vec<OutputWithoutScriptpubkey>) -> Self {
        Self { selected_change }
    }
}

/// Change selection strategy.
pub(crate) trait ChangeSelector {
    type Error;

    fn select(
        &self,
        request: InputSelection,
        dust_limit: Amount,
    ) -> Result<ChangeSelection, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_selection_holds_the_change_it_is_built_with() {
        let change = vec![OutputWithoutScriptpubkey(Amount::from_sat(1_000))];

        assert_eq!(ChangeSelection::new(change.clone()).selected_change, change);
    }
}
